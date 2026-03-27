"""
Informatica IICS Taskflow/Mapping XML → dbt (DuckDB) Converter
==============================================================
Parses Informatica taskflow XMLs, unwinds the mapping DAG,
and generates:
  - dbt SQL models  (models/<name>.sql)
  - dbt YAML schema (models/schema.yml)
  - dbt project file (dbt_project.yml)
  - sources YAML    (models/sources.yml)
  - DuckDB seed DDL (seeds/create_raw_tables.sql)

Usage:
    python informatica_to_dbt.py --xml path/to/taskflow.xml [--out ./dbt_output]
    python informatica_to_dbt.py --xml dir/with/xmls/    [--out ./dbt_output]
"""

import argparse
import os
import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

# ---------------------------------------------------------------------------
# Namespace map (covers IICS taskflow + mapping schemas)
# ---------------------------------------------------------------------------
NS = {
    "sf":   "http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd",
    "tfm":  "http://schemas.active-endpoints.com/appmodules/screenflow/2021/04/taskflowModel.xsd",
    "host": "http://schemas.active-endpoints.com/appmodules/screenflow/2011/06/avosHostEnvironment.xsd",
    "repo": "http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd",
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class MappingStep:
    step_id: str
    title: str
    task_name: str
    guid: str
    task_type: str                     # MCT, DSS, etc.
    wait: bool = True
    max_wait: int = 604800
    outputs: dict = field(default_factory=dict)   # field_name -> label
    error_handler: str = "suspend"     # suspend | ignore | continue
    next_step: Optional[str] = None


@dataclass
class Taskflow:
    name: str
    description: str
    steps: list[MappingStep] = field(default_factory=list)
    step_order: list[str] = field(default_factory=list)   # ordered step IDs


# ---------------------------------------------------------------------------
# XML Parser
# ---------------------------------------------------------------------------
class InformaticaParser:
    """Parse an IICS taskflow XML into a Taskflow object."""

    def parse_file(self, xml_path: str) -> Taskflow:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Unwrap <types1:Entry> if present (repository response wrapper)
        entry = root.find(".//{%s}Entry" % NS["repo"])
        tf_root = entry.find("{%s}taskflow" % NS["sf"]) if entry is not None else None
        if tf_root is None:
            tf_root = root.find(".//{%s}taskflow" % NS["sf"])
        if tf_root is None:
            raise ValueError(f"No <taskflow> element found in {xml_path}")

        name = tf_root.get("name", Path(xml_path).stem)
        desc_el = tf_root.find("{%s}description" % NS["sf"])
        description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""

        tf = Taskflow(name=name, description=description)
        self._parse_flow(tf_root, tf)
        return tf

    # ------------------------------------------------------------------
    def _parse_flow(self, tf_root: ET.Element, tf: Taskflow):
        flow = tf_root.find("{%s}flow" % NS["sf"])
        if flow is None:
            return

        # Build a map of id -> eventContainer
        containers: dict[str, ET.Element] = {}
        for ec in flow.findall("{%s}eventContainer" % NS["sf"]):
            containers[ec.get("id")] = ec

        # Resolve execution order by following links from <start>
        start = flow.find("{%s}start" % NS["sf"])
        ordered_ids = []
        if start is not None:
            cur_id = self._follow_link(start)
            while cur_id and cur_id in containers:
                ordered_ids.append(cur_id)
                ec = containers[cur_id]
                cur_id = self._follow_link(ec)

        tf.step_order = ordered_ids

        # Parse each container
        for ec_id in ordered_ids:
            ec = containers[ec_id]
            step = self._parse_event_container(ec_id, ec)
            if step:
                tf.steps.append(step)

    def _follow_link(self, el: ET.Element) -> Optional[str]:
        link = el.find("{%s}link" % NS["sf"])
        return link.get("targetId") if link is not None else None

    def _parse_event_container(self, ec_id: str, ec: ET.Element) -> Optional[MappingStep]:
        svc = ec.find("{%s}service" % NS["sf"])
        if svc is None:
            return None

        title = (svc.findtext("{%s}title" % NS["sf"]) or "").strip()
        service_name = (svc.findtext("{%s}serviceName" % NS["sf"]) or "").strip()

        # Parse service input parameters
        params = {}
        si = svc.find("{%s}serviceInput" % NS["sf"])
        if si is not None:
            for p in si.findall("{%s}parameter" % NS["sf"]):
                params[p.get("name", "")] = p.text or ""

        task_name = params.get("Task Name", "")
        guid      = params.get("GUID", "")
        task_type = params.get("Task Type", "MCT")
        wait      = params.get("Wait for Task to Complete", "true").lower() == "true"
        max_wait  = int(params.get("Max Wait", "604800"))

        # Parse service output field mappings
        outputs = {}
        so = svc.find("{%s}serviceOutput" % NS["sf"])
        if so is not None:
            for op in so.findall("{%s}operation" % NS["sf"]):
                to_field = op.get("to", "")
                label    = op.text or ""
                # Strip "temp.<step>/output/" prefix to get column name
                col = re.sub(r"^temp\.[^/]+/output/", "", to_field)
                outputs[col] = label

        # Check error handler on catch events
        error_handler = "suspend"
        events = ec.find("{%s}events" % NS["sf"])
        if events is not None:
            for catch in events.findall("{%s}catch" % NS["sf"]):
                if catch.get("name") == "error":
                    error_handler = "suspend" if catch.find("{%s}suspend" % NS["sf"]) is not None else "continue"

        # Determine next step
        next_step = self._follow_link(ec)

        return MappingStep(
            step_id=ec_id,
            title=title or task_name,
            task_name=task_name,
            guid=guid,
            task_type=task_type,
            wait=wait,
            max_wait=max_wait,
            outputs=outputs,
            error_handler=error_handler,
            next_step=next_step,
        )


# ---------------------------------------------------------------------------
# dbt / DuckDB Generator
# ---------------------------------------------------------------------------
class DbtGenerator:
    """Convert parsed Taskflow objects into dbt project files."""

    # Standard ICS execution output columns and their dbt types
    ICS_OUTPUT_COLS: dict[str, str] = {
        "Object_Name":       "varchar",
        "Run_Id":            "bigint",
        "Log_Id":            "bigint",
        "Task_Id":           "varchar",
        "Task_Status":       "varchar",
        "Success_Source_Rows": "bigint",
        "Failed_Source_Rows":  "bigint",
        "Success_Target_Rows": "bigint",
        "Failed_Target_Rows":  "bigint",
        "Start_Time":        "timestamp",
        "End_Time":          "timestamp",
        "Error_Message":     "varchar",
        "TotalTransErrors":  "bigint",
        "FirstErrorCode":    "varchar",
    }

    def __init__(self, out_dir: str = "./dbt_output"):
        self.out = Path(out_dir)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    def generate(self, taskflows: list[Taskflow]):
        (self.out / "models").mkdir(parents=True, exist_ok=True)
        (self.out / "seeds").mkdir(parents=True, exist_ok=True)
        (self.out / "macros").mkdir(parents=True, exist_ok=True)

        self._write_dbt_project(taskflows)
        self._write_profiles()
        self._write_macro_run_task()

        all_sources: list[dict] = []
        all_models_meta: list[dict] = []

        for tf in taskflows:
            model_meta = self._write_tf_model(tf)
            all_models_meta.extend(model_meta)
            all_sources.extend(self._collect_sources(tf))

        self._write_schema_yml(all_models_meta)
        self._write_sources_yml(all_sources)
        self._write_seed_ddl(taskflows)

        print(f"\n✅  Generated dbt project in: {self.out.resolve()}")
        print(f"   models/          — {len(all_models_meta)} SQL model(s)")
        print(f"   seeds/           — raw staging DDL")
        print(f"   dbt_project.yml  — project config")
        print(f"   profiles.yml     — DuckDB connection")
        print(f"   macros/          — run_ics_task macro")

    # ------------------------------------------------------------------
    # dbt_project.yml
    # ------------------------------------------------------------------
    def _write_dbt_project(self, taskflows: list[Taskflow]):
        names = [tf.name for tf in taskflows]
        yml = textwrap.dedent(f"""\
            name: informatica_to_dbt
            version: "1.0.0"
            config-version: 2

            profile: duckdb_local

            model-paths: ["models"]
            seed-paths:  ["seeds"]
            macro-paths: ["macros"]

            target-path: "target"
            clean-targets: ["target", "dbt_packages"]

            models:
              informatica_to_dbt:
                +materialized: table
                staging:
                  +materialized: view
                intermediate:
                  +materialized: ephemeral
                marts:
                  +materialized: table

            # Source taskflows converted: {', '.join(names)}
        """)
        (self.out / "dbt_project.yml").write_text(yml)

    # ------------------------------------------------------------------
    # profiles.yml  (DuckDB)
    # ------------------------------------------------------------------
    def _write_profiles(self):
        yml = textwrap.dedent("""\
            duckdb_local:
              target: dev
              outputs:
                dev:
                  type: duckdb
                  path: "dev.duckdb"          # local file-based DB
                  schema: main
                  threads: 4
                prod:
                  type: duckdb
                  path: "prod.duckdb"
                  schema: main
                  threads: 8
        """)
        (self.out / "profiles.yml").write_text(yml)

    # ------------------------------------------------------------------
    # Macro: run_ics_task  (mimics ICSExecuteDataTask behaviour in DuckDB)
    # ------------------------------------------------------------------
    def _write_macro_run_task(self):
        macro = textwrap.dedent("""\
            {#
              Macro: run_ics_task
              Emulates ICSExecuteDataTask output metadata in DuckDB.
              In a real pipeline replace the body with your ELT execution logic.
            #}
            {% macro run_ics_task(task_name, guid, task_type='MCT') %}
                select
                    '{{ task_name }}'                       as object_name,
                    {{ var('run_id',  0) }}::bigint         as run_id,
                    {{ var('log_id',  0) }}::bigint         as log_id,
                    '{{ guid }}'                            as task_id,
                    'SUCCEEDED'                             as task_status,
                    0::bigint                               as success_source_rows,
                    0::bigint                               as failed_source_rows,
                    0::bigint                               as success_target_rows,
                    0::bigint                               as failed_target_rows,
                    current_timestamp                       as start_time,
                    current_timestamp                       as end_time,
                    null::varchar                           as error_message,
                    0::bigint                               as total_trans_errors,
                    null::varchar                           as first_error_code
            {% endmacro %}
        """)
        (self.out / "macros" / "run_ics_task.sql").write_text(macro)

    # ------------------------------------------------------------------
    # Per-taskflow model(s)
    # ------------------------------------------------------------------
    def _write_tf_model(self, tf: Taskflow) -> list[dict]:
        """
        Generates one staging model per mapping step PLUS one
        orchestration model that unions the audit rows.
        """
        meta: list[dict] = []

        for step in tf.steps:
            model_name = f"stg_{self._snake(step.task_name)}"
            sql = self._step_to_sql(step, tf.name)
            path = self.out / "models" / f"{model_name}.sql"
            path.write_text(sql)
            meta.append({
                "name": model_name,
                "description": f"Staging model for Informatica task '{step.task_name}' "
                               f"(GUID: {step.guid}) from taskflow '{tf.name}'.",
                "columns": list(self.ICS_OUTPUT_COLS.keys()),
            })

        # Orchestration / audit roll-up model
        orch_name = f"int_{self._snake(tf.name)}_audit"
        orch_sql  = self._orchestration_sql(tf)
        (self.out / "models" / f"{orch_name}.sql").write_text(orch_sql)
        meta.append({
            "name": orch_name,
            "description": tf.description or f"Audit roll-up for taskflow '{tf.name}'.",
            "columns": ["step_order", "task_name"] + list(self.ICS_OUTPUT_COLS.keys()),
        })

        return meta

    def _step_to_sql(self, step: MappingStep, tf_name: str) -> str:
        col_defs = ",\n    ".join(
            f"{col}::{dtype}  as {col.lower()}"
            for col, dtype in self.ICS_OUTPUT_COLS.items()
        )
        return textwrap.dedent(f"""\
            {{# ---------------------------------------------------------------
               Model  : stg_{self._snake(step.task_name)}
               Source : Informatica taskflow '{tf_name}'
               Task   : {step.task_name}  (GUID: {step.guid})
               Type   : {step.task_type}
               Error  : {step.error_handler}
            --------------------------------------------------------------- #}}

            {{{{ config(materialized='view') }}}}

            with ics_raw as (

                -- Emulates the output of ICSExecuteDataTask.
                -- Replace this CTE with your actual source table / API result.
                {{{{ run_ics_task(
                    task_name = '{step.task_name}',
                    guid      = '{step.guid}',
                    task_type = '{step.task_type}'
                ) }}}}

            ),

            typed as (

                select
                    {col_defs}
                from ics_raw

            )

            select * from typed
        """)

    def _orchestration_sql(self, tf: Taskflow) -> str:
        unions = []
        for i, step in enumerate(tf.steps, start=1):
            model_ref = f"stg_{self._snake(step.task_name)}"
            unions.append(
                f"    select {i} as step_order, '{step.task_name}' as task_name, * "
                f"from {{{{ ref('{model_ref}') }}}}"
            )
        union_sql = "\n    union all\n".join(unions) if unions else "    select null as step_order, null as task_name"

        return textwrap.dedent(f"""\
            {{# ---------------------------------------------------------------
               Model  : int_{self._snake(tf.name)}_audit
               Purpose: Ordered audit roll-up for all steps in taskflow
                        '{tf.name}'
            --------------------------------------------------------------- #}}

            {{{{ config(materialized='table') }}}}

            with steps as (

            {union_sql}

            )

            select
                step_order,
                task_name,
                task_status,
                success_source_rows,
                failed_source_rows,
                success_target_rows,
                failed_target_rows,
                total_trans_errors,
                first_error_code,
                error_message,
                start_time,
                end_time,
                datediff('second', start_time, end_time) as duration_seconds,
                run_id,
                log_id,
                task_id,
                object_name
            from steps
            order by step_order
        """)

    # ------------------------------------------------------------------
    # schema.yml
    # ------------------------------------------------------------------
    def _write_schema_yml(self, models_meta: list[dict]):
        lines = ["version: 2", "", "models:"]
        for m in models_meta:
            lines.append(f"  - name: {m['name']}")
            lines.append(f"    description: >")
            lines.append(f"      {m['description']}")
            lines.append(f"    columns:")
            for col in m["columns"]:
                lines.append(f"      - name: {col.lower()}")
                if col in ("Run_Id", "Log_Id"):
                    lines.append(f"        tests:")
                    lines.append(f"          - not_null")
                if col == "Task_Status":
                    lines.append(f"        tests:")
                    lines.append(f"          - accepted_values:")
                    lines.append(f"              values: ['SUCCEEDED', 'FAILED', 'RUNNING', 'STOPPED']")
            lines.append("")
        (self.out / "models" / "schema.yml").write_text("\n".join(lines))

    # ------------------------------------------------------------------
    # sources.yml
    # ------------------------------------------------------------------
    def _collect_sources(self, tf: Taskflow) -> list[dict]:
        return [
            {
                "name": self._snake(step.task_name),
                "description": f"Raw landing table for Informatica task {step.task_name}",
                "task_name": step.task_name,
                "guid": step.guid,
            }
            for step in tf.steps
        ]

    def _write_sources_yml(self, sources: list[dict]):
        lines = [
            "version: 2",
            "",
            "sources:",
            "  - name: cpdb_prov_ib",
            "    description: CPDB PROV_IB raw landing schema (DuckDB)",
            "    database: dev",
            "    schema: raw",
            "    tables:",
        ]
        for s in sources:
            lines.append(f"      - name: {s['name']}")
            lines.append(f"        description: \"{s['description']}\"")
            lines.append(f"        meta:")
            lines.append(f"          informatica_task: {s['task_name']}")
            lines.append(f"          guid: {s['guid']}")
        (self.out / "models" / "sources.yml").write_text("\n".join(lines))

    # ------------------------------------------------------------------
    # DuckDB seed DDL
    # ------------------------------------------------------------------
    def _write_seed_ddl(self, taskflows: list[Taskflow]):
        col_ddl = ",\n    ".join(
            f"{col.lower()}  {dtype}"
            for col, dtype in self.ICS_OUTPUT_COLS.items()
        )
        blocks = [
            "-- Auto-generated DuckDB DDL for Informatica task audit tables",
            "-- Run once to initialise the raw schema\n",
            "CREATE SCHEMA IF NOT EXISTS raw;\n",
        ]
        for tf in taskflows:
            for step in tf.steps:
                tbl = f"raw.{self._snake(step.task_name)}"
                blocks.append(textwrap.dedent(f"""\
                    -- Task: {step.task_name}  |  GUID: {step.guid}
                    CREATE TABLE IF NOT EXISTS {tbl} (
                        {col_ddl},
                        _loaded_at  timestamp default current_timestamp
                    );
                """))
        (self.out / "seeds" / "create_raw_tables.sql").write_text("\n".join(blocks))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _snake(name: str) -> str:
        """Convert CamelCase / mixed to snake_case."""
        s = re.sub(r"[-\s]+", "_", name)
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
        s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
        return s.lower().strip("_")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def collect_xml_files(path: str) -> list[str]:
    p = Path(path)
    if p.is_file():
        return [str(p)]
    elif p.is_dir():
        return [str(f) for f in sorted(p.glob("**/*.xml"))]
    else:
        raise FileNotFoundError(f"Path not found: {path}")


def main():
    ap = argparse.ArgumentParser(
        description="Convert Informatica IICS taskflow XML(s) to dbt/DuckDB project"
    )
    ap.add_argument("--xml", required=True,
                    help="Path to a single XML file or directory containing XML files")
    ap.add_argument("--out", default="./dbt_output",
                    help="Output directory for the generated dbt project (default: ./dbt_output)")
    args = ap.parse_args()

    xml_files = collect_xml_files(args.xml)
    print(f"Found {len(xml_files)} XML file(s) to process.\n")

    parser  = InformaticaParser()
    taskflows: list[Taskflow] = []

    for xf in xml_files:
        print(f"  Parsing: {xf}")
        try:
            tf = parser.parse_file(xf)
            print(f"    → {tf.name}  ({len(tf.steps)} step(s))")
            for s in tf.steps:
                print(f"       [{s.step_id}] {s.title}  ({s.task_type})")
            taskflows.append(tf)
        except Exception as e:
            print(f"    ⚠  Skipped ({e})")

    if not taskflows:
        print("\nNo taskflows parsed. Exiting.")
        return

    gen = DbtGenerator(out_dir=args.out)
    gen.generate(taskflows)


if __name__ == "__main__":
    main()
