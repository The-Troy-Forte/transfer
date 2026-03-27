"""
Informatica PowerMart XML + IICS Taskflow XML → dbt (DuckDB) Converter
=======================================================================
Parses:
  - PowerMart POWERMART XMLs  (mappings, mapplets, sources, targets)
  - IICS Taskflow XMLs        (step DAG, execution order)
  - IICS Project JSON         (project/folder metadata)

Generates:
  - dbt SQL models            models/<name>.sql
  - dbt schema YAML           models/schema.yml
  - dbt sources YAML          models/sources.yml
  - dbt project file          dbt_project.yml
  - DuckDB profiles           profiles.yml
  - Jinja macros              macros/
  - Seed DDL                  seeds/create_raw_tables.sql
  - Lineage report            lineage_report.md

Usage:
    python informatica_to_dbt.py --xml path/to/file_or_dir [--out ./dbt_output]
    python informatica_to_dbt.py --xml mappings/ --json project.json [--out ./dbt_output]
"""

import argparse
import json
import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

# ---------------------------------------------------------------------------
# IICS Taskflow namespace map
# ---------------------------------------------------------------------------
NS = {
    "sf":   "http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd",
    "tfm":  "http://schemas.active-endpoints.com/appmodules/screenflow/2021/04/taskflowModel.xsd",
    "host": "http://schemas.active-endpoints.com/appmodules/screenflow/2011/06/avosHostEnvironment.xsd",
    "repo": "http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd",
}

# ---------------------------------------------------------------------------
# PowerMart type → DuckDB type
# ---------------------------------------------------------------------------
INFA_TYPE_MAP: dict[str, str] = {
    "string":        "varchar",
    "nstring":       "varchar",
    "char":          "varchar",
    "nchar":         "varchar",
    "text":          "varchar",
    "double":        "double",
    "decimal":       "decimal(38,10)",
    "integer":       "integer",
    "smallinteger":  "smallint",
    "bigint":        "bigint",
    "real":          "float",
    "float":         "float",
    "date/time":     "timestamp",
    "date":          "date",
    "time":          "time",
    "binary":        "blob",
}

# ---------------------------------------------------------------------------
# Data classes — Mapping layer
# ---------------------------------------------------------------------------
@dataclass
class TransformField:
    name: str
    datatype: str
    port_type: str          # INPUT | OUTPUT | INPUT/OUTPUT | LOCAL VARIABLE | LOOKUP | LOOKUP/RETURN/OUTPUT
    expression: str = ""
    precision: int = 4000
    scale: int = 0
    default_value: str = ""
    description: str = ""

    @property
    def duckdb_type(self) -> str:
        return INFA_TYPE_MAP.get(self.datatype.lower(), "varchar")

    @property
    def is_input(self) -> bool:
        return "INPUT" in self.port_type and "OUTPUT" not in self.port_type

    @property
    def is_output(self) -> bool:
        return "OUTPUT" in self.port_type

    @property
    def is_local(self) -> bool:
        return "LOCAL VARIABLE" in self.port_type


@dataclass
class Connector:
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str


@dataclass
class Transformation:
    name: str
    ttype: str              # Expression | Lookup Procedure | Stored Procedure | Input Transformation | Output Transformation | Filter | Mapplet | ...
    fields: list[TransformField] = field(default_factory=list)
    attributes: dict[str, str] = field(default_factory=dict)
    description: str = ""
    reusable: bool = False

    # convenience accessors
    @property
    def inputs(self) -> list[TransformField]:
        return [f for f in self.fields if f.is_input]

    @property
    def outputs(self) -> list[TransformField]:
        return [f for f in self.fields if f.is_output]

    @property
    def locals(self) -> list[TransformField]:
        return [f for f in self.fields if f.is_local]

    @property
    def lookup_table(self) -> str:
        return self.attributes.get("Lookup table name", "")

    @property
    def lookup_sql(self) -> str:
        return self.attributes.get("Lookup Sql Override", "")

    @property
    def lookup_condition(self) -> str:
        return self.attributes.get("Lookup condition", "")

    @property
    def filter_condition(self) -> str:
        return self.attributes.get("Filter Condition", "")

    @property
    def sp_name(self) -> str:
        return self.attributes.get("Stored Procedure Name", self.name)


@dataclass
class MappingInstance:
    name: str
    transformation_name: str
    transformation_type: str
    reusable: bool = False


@dataclass
class Mapping:
    name: str
    folder: str
    description: str = ""
    transformations: dict[str, Transformation] = field(default_factory=dict)
    instances: dict[str, MappingInstance] = field(default_factory=dict)
    connectors: list[Connector] = field(default_factory=list)
    is_mapplet: bool = False

    def topo_sort(self) -> list[str]:
        """Return instance names in topological execution order."""
        deps: dict[str, set] = {n: set() for n in self.instances}
        for c in self.connectors:
            if c.to_instance in deps:
                deps[c.to_instance].add(c.from_instance)
        ordered, visited = [], set()

        def visit(n):
            if n in visited:
                return
            visited.add(n)
            for d in deps.get(n, []):
                visit(d)
            ordered.append(n)

        for n in list(self.instances):
            visit(n)
        return ordered


# ---------------------------------------------------------------------------
# Data classes — Taskflow layer
# ---------------------------------------------------------------------------
@dataclass
class MappingStep:
    step_id: str
    title: str
    task_name: str
    guid: str
    task_type: str
    wait: bool = True
    max_wait: int = 604800
    outputs: dict = field(default_factory=dict)
    error_handler: str = "suspend"
    next_step: Optional[str] = None


@dataclass
class Taskflow:
    name: str
    description: str
    steps: list[MappingStep] = field(default_factory=list)
    step_order: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# PowerMart XML Parser
# ---------------------------------------------------------------------------
class PowerMartParser:
    """Parse a PowerMart POWERMART XML into Mapping / Mapplet objects."""

    def parse_file(self, xml_path: str) -> list[Mapping]:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        mappings: list[Mapping] = []

        for repo in root.iter("REPOSITORY"):
            for folder in repo.iter("FOLDER"):
                folder_name = folder.get("NAME", "")
                # Mapplets
                for mlt in folder.iter("MAPPLET"):
                    m = self._parse_mapping_element(mlt, folder_name, is_mapplet=True)
                    if m:
                        mappings.append(m)
                # Mappings (skip if inside a MAPPLET already counted)
                for mapping in folder.findall("MAPPING"):
                    m = self._parse_mapping_element(mapping, folder_name, is_mapplet=False)
                    if m:
                        mappings.append(m)

        return mappings

    # ------------------------------------------------------------------
    def _parse_mapping_element(self, el: ET.Element, folder: str, is_mapplet: bool) -> Optional[Mapping]:
        name = el.get("NAME", "")
        desc = el.get("DESCRIPTION", "")
        m = Mapping(name=name, folder=folder, description=desc, is_mapplet=is_mapplet)

        # Transformations
        for t in el.findall("TRANSFORMATION"):
            trans = self._parse_transformation(t)
            m.transformations[trans.name] = trans

        # Instances
        for inst in el.findall("INSTANCE"):
            i = MappingInstance(
                name=inst.get("NAME", ""),
                transformation_name=inst.get("TRANSFORMATION_NAME", inst.get("NAME", "")),
                transformation_type=inst.get("TRANSFORMATION_TYPE", ""),
                reusable=inst.get("REUSABLE", "NO").upper() == "YES",
            )
            m.instances[i.name] = i

        # If no explicit INSTANCE blocks, synthesise from TRANSFORMATION tags
        if not m.instances:
            for tname, trans in m.transformations.items():
                m.instances[tname] = MappingInstance(
                    name=tname,
                    transformation_name=tname,
                    transformation_type=trans.ttype,
                )

        # Connectors
        for c in el.findall("CONNECTOR"):
            m.connectors.append(Connector(
                from_instance=c.get("FROMINSTANCE", ""),
                from_field=c.get("FROMFIELD", ""),
                to_instance=c.get("TOINSTANCE", ""),
                to_field=c.get("TOFIELD", ""),
            ))

        return m

    def _parse_transformation(self, el: ET.Element) -> Transformation:
        t = Transformation(
            name=el.get("NAME", ""),
            ttype=el.get("TYPE", ""),
            description=el.get("DESCRIPTION", ""),
            reusable=el.get("REUSABLE", "NO").upper() == "YES",
        )
        for tf in el.findall("TRANSFORMFIELD"):
            t.fields.append(TransformField(
                name=tf.get("NAME", ""),
                datatype=tf.get("DATATYPE", "string"),
                port_type=tf.get("PORTTYPE", "INPUT"),
                expression=tf.get("EXPRESSION", ""),
                precision=int(tf.get("PRECISION", 4000)),
                scale=int(tf.get("SCALE", 0)),
                default_value=tf.get("DEFAULTVALUE", ""),
                description=tf.get("DESCRIPTION", ""),
            ))
        for ta in el.findall("TABLEATTRIBUTE"):
            t.attributes[ta.get("NAME", "")] = ta.get("VALUE", "")
        return t


# ---------------------------------------------------------------------------
# IICS Taskflow Parser  (unchanged from original)
# ---------------------------------------------------------------------------
class InformaticaTaskflowParser:

    def parse_file(self, xml_path: str) -> Taskflow:
        tree = ET.parse(xml_path)
        root = tree.getroot()

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

    def _parse_flow(self, tf_root, tf):
        flow = tf_root.find("{%s}flow" % NS["sf"])
        if flow is None:
            return
        containers = {ec.get("id"): ec for ec in flow.findall("{%s}eventContainer" % NS["sf"])}
        start = flow.find("{%s}start" % NS["sf"])
        ordered_ids = []
        if start is not None:
            cur_id = self._follow_link(start)
            while cur_id and cur_id in containers:
                ordered_ids.append(cur_id)
                cur_id = self._follow_link(containers[cur_id])
        tf.step_order = ordered_ids
        for ec_id in ordered_ids:
            step = self._parse_event_container(ec_id, containers[ec_id])
            if step:
                tf.steps.append(step)

    def _follow_link(self, el) -> Optional[str]:
        link = el.find("{%s}link" % NS["sf"])
        return link.get("targetId") if link is not None else None

    def _parse_event_container(self, ec_id, ec) -> Optional[MappingStep]:
        svc = ec.find("{%s}service" % NS["sf"])
        if svc is None:
            return None
        title = (svc.findtext("{%s}title" % NS["sf"]) or "").strip()
        params = {}
        si = svc.find("{%s}serviceInput" % NS["sf"])
        if si is not None:
            for p in si.findall("{%s}parameter" % NS["sf"]):
                params[p.get("name", "")] = p.text or ""
        outputs = {}
        so = svc.find("{%s}serviceOutput" % NS["sf"])
        if so is not None:
            for op in so.findall("{%s}operation" % NS["sf"]):
                col = re.sub(r"^temp\.[^/]+/output/", "", op.get("to", ""))
                outputs[col] = op.text or ""
        error_handler = "suspend"
        events = ec.find("{%s}events" % NS["sf"])
        if events is not None:
            for catch in events.findall("{%s}catch" % NS["sf"]):
                if catch.get("name") == "error":
                    error_handler = "suspend" if catch.find("{%s}suspend" % NS["sf"]) is not None else "continue"
        return MappingStep(
            step_id=ec_id,
            title=title or params.get("Task Name", ""),
            task_name=params.get("Task Name", ""),
            guid=params.get("GUID", ""),
            task_type=params.get("Task Type", "MCT"),
            wait=params.get("Wait for Task to Complete", "true").lower() == "true",
            max_wait=int(params.get("Max Wait", "604800")),
            outputs=outputs,
            error_handler=error_handler,
            next_step=self._follow_link(ec),
        )


# ---------------------------------------------------------------------------
# Project JSON Parser
# ---------------------------------------------------------------------------
class ProjectJsonParser:
    """Parse an IICS OData project JSON export."""

    def parse_file(self, json_path: str) -> dict:
        with open(json_path) as f:
            data = json.load(f)
        props = {p["name"]: p["value"] for p in data.get("properties", []) if "name" in p}
        parent_info = []
        for pi_prop in data.get("properties", []):
            if pi_prop.get("name") == "parentInfo" and pi_prop.get("collection"):
                for item in pi_prop.get("value", []):
                    vals = {v["name"]: v["value"] for v in item.get("value", []) if "name" in v}
                    parent_info.append(vals)
        return {
            "id":          props.get("id", ""),
            "name":        props.get("name", ""),
            "description": props.get("description", ""),
            "owner":       props.get("owner", ""),
            "created":     props.get("createdTime", ""),
            "modified":    props.get("lastUpdatedTime", ""),
            "state":       props.get("documentState", ""),
            "parents":     parent_info,
        }


# ---------------------------------------------------------------------------
# Expression Transpiler  (Informatica → DuckDB SQL)
# ---------------------------------------------------------------------------
class ExpressionTranspiler:
    """
    Best-effort transpilation of Informatica expressions to DuckDB SQL.
    Handles the most common functions; unknown calls are left as comments.
    """

    # Map Informatica function → DuckDB equivalent
    FUNC_MAP = {
        "ISNULL":     "({0} IS NULL)",
        "ISNULL_NOT": "({0} IS NOT NULL)",
        "NVL":        "COALESCE({0}, {1})",
        "NVL2":       "CASE WHEN {0} IS NOT NULL THEN {1} ELSE {2} END",
        "UPPER":      "UPPER({0})",
        "LOWER":      "LOWER({0})",
        "LTRIM":      "LTRIM({0})",
        "RTRIM":      "RTRIM({0})",
        "TRIM":       "TRIM({0})",
        "LENGTH":     "LENGTH({0})",
        "SUBSTR":     "SUBSTRING({0}, {1}, {2})",
        "INSTR":      "INSTR({0}, {1})",
        "LPAD":       "LPAD({0}, {1}, {2})",
        "RPAD":       "RPAD({0}, {1}, {2})",
        "REPLACE":    "REPLACE({0}, {1}, {2})",
        "CONCAT":     "CONCAT({0}, {1})",
        "TO_CHAR":    "CAST({0} AS VARCHAR)",
        "TO_DATE":    "STRPTIME({0}, {1})",
        "TO_INTEGER": "CAST({0} AS INTEGER)",
        "TO_DECIMAL": "CAST({0} AS DECIMAL)",
        "TO_FLOAT":   "CAST({0} AS FLOAT)",
        "DECODE":     "CASE {0} {1} END",
        "IN":         "({0} IN ({1}))",
        "ABS":        "ABS({0})",
        "CEIL":       "CEIL({0})",
        "FLOOR":      "FLOOR({0})",
        "MOD":        "MOD({0}, {1})",
        "ROUND":      "ROUND({0}, {1})",
        "TRUNC":      "TRUNC({0})",
        "SYSDATE":    "CURRENT_TIMESTAMP",
        "SYSTIMESTAMP": "CURRENT_TIMESTAMP",
        "ADD_TO_DATE": "({0} + INTERVAL '{1}' {2})",
        "LAST_DAY":   "LAST_DAY({0})",
        "DATE_DIFF":  "DATEDIFF('{2}', {0}, {1})",
        "GET_DATE_PART": "DATE_PART('{1}', {0})",
        "MAX":        "MAX({0})",
        "MIN":        "MIN({0})",
        "SUM":        "SUM({0})",
        "COUNT":      "COUNT({0})",
        "AVG":        "AVG({0})",
        "ERROR":      "NULL  /* ERROR({0}) */",
        "ABORT":      "NULL  /* ABORT({0}) */",
    }

    def transpile(self, expr: str) -> str:
        if not expr:
            return ""
        out = expr

        # Clean up XML entities
        out = out.replace("&apos;", "'").replace("&amp;", "&").replace("&#xD;&#xA;", "\n")

        # Strip stored-procedure prefix :SP.
        out = re.sub(r":SP\.", "", out)

        # IIF → CASE WHEN
        out = self._iif_to_case(out)

        # Simple function rewrites
        for infa_fn, duckdb_fn in self.FUNC_MAP.items():
            out = re.sub(
                rf"\b{infa_fn}\s*\(",
                lambda m, fn=duckdb_fn: f"__REPLACE__{fn.split('(')[0]}(",
                out,
                flags=re.IGNORECASE,
            )
        out = out.replace("__REPLACE__", "")

        # DECODE(col, val1, res1, val2, res2, ..., default)
        out = self._decode_to_case(out)

        return out.strip()

    @staticmethod
    def _iif_to_case(expr: str) -> str:
        """Convert IIF(cond, true_val, false_val) → CASE WHEN cond THEN true_val ELSE false_val END."""
        pattern = re.compile(r"\bIIF\s*\(", re.IGNORECASE)
        result = []
        i = 0
        while i < len(expr):
            m = pattern.search(expr, i)
            if not m:
                result.append(expr[i:])
                break
            result.append(expr[i:m.start()])
            # Walk to find matching paren and extract args
            depth, start = 1, m.end()
            args_chars = []
            j = start
            while j < len(expr) and depth > 0:
                ch = expr[j]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                if depth > 0:
                    args_chars.append(ch)
                j += 1
            args_str = "".join(args_chars)
            args = ExpressionTranspiler._split_args(args_str)
            if len(args) >= 3:
                result.append(f"CASE WHEN {args[0].strip()} THEN {args[1].strip()} ELSE {args[2].strip()} END")
            else:
                result.append(f"IIF({args_str})")
            i = j
        return "".join(result)

    @staticmethod
    def _decode_to_case(expr: str) -> str:
        return expr  # pass-through; full DECODE parsing left as extension

    @staticmethod
    def _split_args(s: str) -> list[str]:
        """Split comma-separated args respecting nested parens/quotes."""
        args, depth, buf = [], 0, []
        for ch in s:
            if ch == "(" :
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                args.append("".join(buf))
                buf = []
            else:
                buf.append(ch)
        if buf:
            args.append("".join(buf))
        return args


# ---------------------------------------------------------------------------
# dbt / DuckDB Generator
# ---------------------------------------------------------------------------
class DbtGenerator:

    ICS_OUTPUT_COLS: dict[str, str] = {
        "Object_Name":         "varchar",
        "Run_Id":              "bigint",
        "Log_Id":              "bigint",
        "Task_Id":             "varchar",
        "Task_Status":         "varchar",
        "Success_Source_Rows": "bigint",
        "Failed_Source_Rows":  "bigint",
        "Success_Target_Rows": "bigint",
        "Failed_Target_Rows":  "bigint",
        "Start_Time":          "timestamp",
        "End_Time":            "timestamp",
        "Error_Message":       "varchar",
        "TotalTransErrors":    "bigint",
        "FirstErrorCode":      "varchar",
    }

    def __init__(self, out_dir: str = "./dbt_output"):
        self.out = Path(out_dir)
        self.transpiler = ExpressionTranspiler()

    # ------------------------------------------------------------------
    def generate(
        self,
        taskflows: list[Taskflow],
        mappings: list[Mapping],
        project_meta: Optional[dict] = None,
    ):
        for d in ("models/staging", "models/intermediate", "models/marts",
                  "seeds", "macros", "analyses"):
            (self.out / d).mkdir(parents=True, exist_ok=True)

        self._write_dbt_project(taskflows, mappings, project_meta)
        self._write_profiles()
        self._write_macros()

        all_models_meta: list[dict] = []
        all_sources: list[dict] = []

        # Mapping models
        for m in mappings:
            meta = self._write_mapping_model(m)
            all_models_meta.extend(meta)
            all_sources.extend(self._collect_mapping_sources(m))

        # Taskflow orchestration models
        for tf in taskflows:
            meta = self._write_tf_model(tf)
            all_models_meta.extend(meta)
            all_sources.extend(self._collect_tf_sources(tf))

        self._write_schema_yml(all_models_meta)
        self._write_sources_yml(all_sources)
        self._write_seed_ddl(taskflows, mappings)
        self._write_lineage_report(mappings, taskflows, project_meta)

        print(f"\n✅  Generated dbt project → {self.out.resolve()}")
        print(f"   models/          — {len(all_models_meta)} SQL model(s)")
        print(f"   seeds/           — raw staging DDL")
        print(f"   macros/          — run_ics_task + lookup helpers")
        print(f"   lineage_report.md — human-readable lineage")

    # ------------------------------------------------------------------
    # dbt_project.yml
    # ------------------------------------------------------------------
    def _write_dbt_project(self, taskflows, mappings, project_meta):
        proj_name = "informatica_to_dbt"
        proj_desc = ""
        if project_meta:
            proj_name = _snake(project_meta.get("name", proj_name))
            proj_desc = f"# {project_meta.get('description', '')}"
        yml = textwrap.dedent(f"""\
            name: {proj_name}
            version: "1.0.0"
            config-version: 2
            {proj_desc}

            profile: duckdb_local

            model-paths: ["models"]
            seed-paths:  ["seeds"]
            macro-paths: ["macros"]
            analysis-paths: ["analyses"]

            target-path: "target"
            clean-targets: ["target", "dbt_packages"]

            vars:
              run_id: 0
              log_id: 0

            models:
              {proj_name}:
                staging:
                  +materialized: view
                  +schema: staging
                intermediate:
                  +materialized: ephemeral
                marts:
                  +materialized: table
                  +schema: marts
        """)
        (self.out / "dbt_project.yml").write_text(yml)

    # ------------------------------------------------------------------
    # profiles.yml
    # ------------------------------------------------------------------
    def _write_profiles(self):
        (self.out / "profiles.yml").write_text(textwrap.dedent("""\
            duckdb_local:
              target: dev
              outputs:
                dev:
                  type: duckdb
                  path: "dev.duckdb"
                  schema: main
                  threads: 4
                prod:
                  type: duckdb
                  path: "prod.duckdb"
                  schema: main
                  threads: 8
        """))

    # ------------------------------------------------------------------
    # Macros
    # ------------------------------------------------------------------
    def _write_macros(self):
        # run_ics_task
        (self.out / "macros" / "run_ics_task.sql").write_text(textwrap.dedent("""\
            {% macro run_ics_task(task_name, guid, task_type='MCT') %}
                select
                    '{{ task_name }}'                  as object_name,
                    {{ var('run_id', 0) }}::bigint     as run_id,
                    {{ var('log_id', 0) }}::bigint     as log_id,
                    '{{ guid }}'                       as task_id,
                    'SUCCEEDED'                        as task_status,
                    0::bigint                          as success_source_rows,
                    0::bigint                          as failed_source_rows,
                    0::bigint                          as success_target_rows,
                    0::bigint                          as failed_target_rows,
                    current_timestamp                  as start_time,
                    current_timestamp                  as end_time,
                    null::varchar                      as error_message,
                    0::bigint                          as total_trans_errors,
                    null::varchar                      as first_error_code
            {% endmacro %}
        """))

        # lookup helper
        (self.out / "macros" / "infa_lookup.sql").write_text(textwrap.dedent("""\
            {#
              Macro: infa_lookup
              Emulates Informatica cached lookup semantics.
              table     - fully qualified table name
              condition - SQL join condition referencing input aliases
              cols      - list of columns to return
            #}
            {% macro infa_lookup(table, condition, cols) %}
                (select {{ cols | join(', ') }}
                 from {{ table }}
                 where {{ condition }}
                 limit 1)
            {% endmacro %}
        """))

        # iif helper
        (self.out / "macros" / "iif.sql").write_text(textwrap.dedent("""\
            {% macro iif(condition, true_val, false_val) %}
                CASE WHEN {{ condition }} THEN {{ true_val }} ELSE {{ false_val }} END
            {% endmacro %}
        """))

    # ------------------------------------------------------------------
    # Mapping → dbt model
    # ------------------------------------------------------------------
    def _write_mapping_model(self, m: Mapping) -> list[dict]:
        meta: list[dict] = []
        prefix = "mplt" if m.is_mapplet else "stg"
        model_name = f"{prefix}_{_snake(m.name)}"
        sql = self._mapping_to_sql(m, model_name)
        subdir = "staging"
        path = self.out / "models" / subdir / f"{model_name}.sql"
        path.write_text(sql)

        # Collect output columns for schema
        out_cols = self._output_columns(m)
        meta.append({
            "name": model_name,
            "description": m.description or f"dbt model generated from Informatica {'mapplet' if m.is_mapplet else 'mapping'} '{m.name}' (folder: {m.folder}).",
            "columns": out_cols,
            "layer": "staging",
        })
        return meta

    def _output_columns(self, m: Mapping) -> list[dict]:
        """Collect final output port definitions."""
        cols = []
        for inst_name in reversed(m.topo_sort()):
            inst = m.instances.get(inst_name)
            if inst is None:
                continue
            t = m.transformations.get(inst.transformation_name)
            if t is None:
                continue
            if t.ttype in ("Output Transformation",):
                for f in t.inputs:
                    cols.append({"name": f.name.lower(), "dtype": f.duckdb_type, "description": f.description})
        if not cols:
            # Fall back: collect all OUTPUT ports from last topo node
            for inst_name in reversed(m.topo_sort()):
                inst = m.instances.get(inst_name)
                if inst is None:
                    continue
                t = m.transformations.get(inst.transformation_name)
                if t and t.outputs:
                    cols = [{"name": f.name.lower(), "dtype": f.duckdb_type, "description": f.description}
                            for f in t.outputs]
                    break
        return cols

    def _mapping_to_sql(self, m: Mapping, model_name: str) -> str:
        ordered = m.topo_sort()

        # Build adjacency: instance → list of upstream (from_instance, from_field, to_field)
        upstream: dict[str, list[Connector]] = {n: [] for n in m.instances}
        for c in m.connectors:
            if c.to_instance in upstream:
                upstream[c.to_instance].append(c)

        header = textwrap.dedent(f"""\
            {{# -----------------------------------------------------------
               Model   : {model_name}
               Source  : Informatica {'Mapplet' if m.is_mapplet else 'Mapping'} '{m.name}'
               Folder  : {m.folder}
               {'Mapplet' if m.is_mapplet else 'Mapping'} desc: {m.description or 'n/a'}
            ----------------------------------------------------------- #}}

            {{{{ config(materialized='view') }}}}

        """)

        ctes: list[str] = []
        final_cte = "final"

        for inst_name in ordered:
            inst = m.instances.get(inst_name)
            if inst is None:
                continue
            t = m.transformations.get(inst.transformation_name)
            if t is None:
                continue
            cte = self._instance_to_cte(inst_name, inst, t, upstream, m)
            if cte:
                ctes.append(cte)

        if not ctes:
            return header + "select 1 as placeholder\n"

        # Determine final select from Output Transformation or last CTE
        final_inst = None
        for inst_name in reversed(ordered):
            inst = m.instances.get(inst_name)
            if inst:
                t = m.transformations.get(inst.transformation_name)
                if t and t.ttype == "Output Transformation":
                    final_inst = inst_name
                    break
        if final_inst is None:
            final_inst = ordered[-1] if ordered else "unknown"

        cte_sql = ",\n\n".join(ctes)
        return header + f"with\n\n{cte_sql}\n\nselect * from {_snake(final_inst)}\n"

    def _instance_to_cte(
        self,
        inst_name: str,
        inst: MappingInstance,
        t: Transformation,
        upstream: dict,
        m: Mapping,
    ) -> str:
        cte_name = _snake(inst_name)
        ups = upstream.get(inst_name, [])
        up_ctes = list(dict.fromkeys(_snake(c.from_instance) for c in ups))  # ordered unique

        # ---------- Input / Source Transformation ----------
        if t.ttype in ("Input Transformation", "Source Qualifier"):
            cols = ", ".join(f.name.lower() for f in t.outputs) or "*"
            src_table = t.attributes.get("Table Name", t.attributes.get("Source table name", inst_name))
            return (
                f"{cte_name} as (\n"
                f"    -- Source: {src_table}\n"
                f"    select {cols}\n"
                f"    from {{{{ source('raw', '{_snake(src_table)}') }}}}\n"
                f")"
            )

        # ---------- Output Transformation ----------
        if t.ttype == "Output Transformation":
            if up_ctes:
                from_clause = up_ctes[0]
                col_map = {c.to_field: c.from_field for c in ups}
                cols = ", ".join(
                    f"{col_map.get(f.name, f.name)} as {f.name.lower()}"
                    for f in t.inputs
                )
                return (
                    f"{cte_name} as (\n"
                    f"    select {cols}\n"
                    f"    from {from_clause}\n"
                    f")"
                )
            return ""

        # ---------- Expression Transformation ----------
        if t.ttype == "Expression":
            from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
            # Inputs passed through
            passthrough = {c.to_field: c.from_field for c in ups}

            select_parts = []
            # Pass-through inputs (no expression)
            for f in t.inputs:
                src = passthrough.get(f.name, f.name)
                select_parts.append(f"    {src} as {f.name.lower()}")

            # Local variables (computed, not exposed)
            local_defs = []
            for f in t.locals:
                expr = self.transpiler.transpile(f.expression)
                local_defs.append(f"    {expr} as {f.name.lower()}  -- local var")

            # Output ports with expressions
            for f in t.outputs:
                expr = self.transpiler.transpile(f.expression) if f.expression else f.name
                select_parts.append(f"    {expr} as {f.name.lower()}")

            all_cols = "\n,".join(select_parts)
            local_sql = ("\n,".join(local_defs) + "\n," if local_defs else "")
            return (
                f"{cte_name} as (\n"
                f"    select\n"
                f"    {local_sql}{all_cols}\n"
                f"    from {from_clause}\n"
                f")"
            )

        # ---------- Lookup Transformation ----------
        if t.ttype == "Lookup Procedure":
            from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
            lkp_table = t.lookup_table or t.attributes.get("Lookup table name", "unknown_lookup")
            lkp_sql_override = t.lookup_sql
            condition = self.transpiler.transpile(t.lookup_condition)
            return_cols = [f.name.lower() for f in t.fields if "RETURN" in f.port_type or "OUTPUT" in f.port_type]
            ret_col_sql = ", ".join(f"lkp.{c}" for c in return_cols) or "lkp.*"

            if lkp_sql_override:
                lkp_src = f"(\n        {lkp_sql_override.strip()}\n    )"
            else:
                lkp_src = f"/* {lkp_table} — replace with {{ source() }} ref */"

            return (
                f"{cte_name} as (\n"
                f"    -- Lookup: {lkp_table}\n"
                f"    -- Condition: {condition}\n"
                f"    select\n"
                f"        src.*,\n"
                f"        {ret_col_sql}\n"
                f"    from {from_clause} src\n"
                f"    left join {lkp_src} lkp\n"
                f"        on {condition if condition else '/* add join condition */'}\n"
                f")"
            )

        # ---------- Filter Transformation ----------
        if t.ttype == "Filter":
            from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
            cond = self.transpiler.transpile(t.filter_condition) or "TRUE"
            return (
                f"{cte_name} as (\n"
                f"    select *\n"
                f"    from {from_clause}\n"
                f"    where {cond}\n"
                f")"
            )

        # ---------- Joiner Transformation ----------
        if t.ttype == "Joiner":
            master = up_ctes[0] if len(up_ctes) > 0 else "master_src"
            detail = up_ctes[1] if len(up_ctes) > 1 else "detail_src"
            join_cond = self.transpiler.transpile(t.attributes.get("Join Condition", "TRUE"))
            join_type = t.attributes.get("Join Type", "Normal").upper()
            sql_join = "INNER JOIN" if "NORMAL" in join_type else "LEFT JOIN"
            return (
                f"{cte_name} as (\n"
                f"    -- Joiner ({join_type})\n"
                f"    select m.*, d.*\n"
                f"    from {master} m\n"
                f"    {sql_join} {detail} d on {join_cond}\n"
                f")"
            )

        # ---------- Aggregator Transformation ----------
        if t.ttype == "Aggregator":
            from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
            group_cols = [f.name.lower() for f in t.inputs if not f.expression]
            agg_cols = [
                f"    {self.transpiler.transpile(f.expression)} as {f.name.lower()}"
                for f in t.outputs if f.expression
            ]
            group_sql = f"    group by {', '.join(group_cols)}" if group_cols else ""
            return (
                f"{cte_name} as (\n"
                f"    select\n"
                f"    {', '.join(group_cols + [])}\n"
                f"    {',' + chr(10).join(agg_cols) if agg_cols else ''}\n"
                f"    from {from_clause}\n"
                f"    {group_sql}\n"
                f")"
            )

        # ---------- Stored Procedure ----------
        if t.ttype == "Stored Procedure":
            sp = t.sp_name
            in_cols = ", ".join(f.name.lower() for f in t.inputs)
            out_cols = ", ".join(f"null::{f.duckdb_type} as {f.name.lower()}" for f in t.outputs)
            from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
            return (
                f"{cte_name} as (\n"
                f"    -- Stored Procedure: {sp}({in_cols})\n"
                f"    -- DuckDB: implement as macro or UDF\n"
                f"    select\n"
                f"        src.*,\n"
                f"        {out_cols}\n"
                f"    from {from_clause} src\n"
                f")"
            )

        # ---------- Mapplet instance ----------
        if t.ttype == "Mapplet":
            ref_name = f"mplt_{_snake(inst.transformation_name)}"
            in_cols = ", ".join(c.from_field.lower() for c in ups)
            from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
            return (
                f"{cte_name} as (\n"
                f"    -- Mapplet: {inst.transformation_name}\n"
                f"    select * from {{{{ ref('{ref_name}') }}}}\n"
                f"    -- TODO: pass inputs ({in_cols}) from {from_clause}\n"
                f")"
            )

        # ---------- Fallback ----------
        from_clause = up_ctes[0] if up_ctes else "/* missing upstream */"
        return (
            f"{cte_name} as (\n"
            f"    -- {t.ttype}: {inst_name} (no explicit handler — pass-through)\n"
            f"    select * from {from_clause}\n"
            f")"
        )

    # ------------------------------------------------------------------
    # Taskflow → dbt models  (audit / orchestration)
    # ------------------------------------------------------------------
    def _write_tf_model(self, tf: Taskflow) -> list[dict]:
        meta: list[dict] = []
        for step in tf.steps:
            model_name = f"stg_{_snake(step.task_name)}"
            sql = self._step_to_sql(step, tf.name)
            (self.out / "models" / "staging" / f"{model_name}.sql").write_text(sql)
            meta.append({
                "name": model_name,
                "description": f"Staging model for Informatica task '{step.task_name}' (GUID: {step.guid}) from taskflow '{tf.name}'.",
                "columns": [{"name": c.lower(), "dtype": d, "description": ""} for c, d in self.ICS_OUTPUT_COLS.items()],
                "layer": "staging",
            })

        orch_name = f"int_{_snake(tf.name)}_audit"
        (self.out / "models" / "intermediate" / f"{orch_name}.sql").write_text(self._orchestration_sql(tf))
        meta.append({
            "name": orch_name,
            "description": tf.description or f"Audit roll-up for taskflow '{tf.name}'.",
            "columns": [{"name": c, "dtype": "varchar", "description": ""} for c in
                        ["step_order", "task_name", "task_status", "duration_seconds"]],
            "layer": "intermediate",
        })
        return meta

    def _step_to_sql(self, step: MappingStep, tf_name: str) -> str:
        col_defs = ",\n    ".join(
            f"{col}::{dtype}  as {col.lower()}"
            for col, dtype in self.ICS_OUTPUT_COLS.items()
        )
        return textwrap.dedent(f"""\
            {{# Model: stg_{_snake(step.task_name)} | Task: {step.task_name} | TF: {tf_name} #}}

            {{{{ config(materialized='view') }}}}

            with ics_raw as (
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
        unions = [
            f"    select {i} as step_order, '{s.task_name}' as task_name, * "
            f"from {{{{ ref('stg_{_snake(s.task_name)}') }}}}"
            for i, s in enumerate(tf.steps, 1)
        ] or ["    select null as step_order, null as task_name"]
        return textwrap.dedent(f"""\
            {{{{ config(materialized='table') }}}}

            with steps as (
            {"    union all".join(unions)}
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
            lines += [
                f"  - name: {m['name']}",
                f"    description: >",
                f"      {m['description']}",
                f"    columns:",
            ]
            for col in m.get("columns", []):
                cname = col["name"] if isinstance(col, dict) else col.lower()
                lines.append(f"      - name: {cname}")
                if cname in ("run_id", "log_id"):
                    lines += ["        tests:", "          - not_null"]
                if cname == "task_status":
                    lines += [
                        "        tests:",
                        "          - accepted_values:",
                        "              values: ['SUCCEEDED','FAILED','RUNNING','STOPPED']",
                    ]
            lines.append("")
        (self.out / "models" / "schema.yml").write_text("\n".join(lines))

    # ------------------------------------------------------------------
    # sources.yml
    # ------------------------------------------------------------------
    def _collect_mapping_sources(self, m: Mapping) -> list[dict]:
        srcs = []
        for inst_name, inst in m.instances.items():
            t = m.transformations.get(inst.transformation_name)
            if t and t.ttype in ("Input Transformation", "Source Qualifier"):
                tbl = t.attributes.get("Table Name", t.attributes.get("Source table name", inst_name))
                srcs.append({"name": _snake(tbl), "description": f"Source for {m.name}/{inst_name}", "raw_table": tbl})
        return srcs

    def _collect_tf_sources(self, tf: Taskflow) -> list[dict]:
        return [{"name": _snake(s.task_name), "description": f"Raw landing for {s.task_name}", "raw_table": s.task_name}
                for s in tf.steps]

    def _write_sources_yml(self, sources: list[dict]):
        seen = set()
        lines = [
            "version: 2", "",
            "sources:",
            "  - name: raw",
            "    description: Raw landing schema (DuckDB)",
            "    database: dev",
            "    schema: raw",
            "    tables:",
        ]
        for s in sources:
            n = s["name"]
            if n in seen:
                continue
            seen.add(n)
            lines += [
                f"      - name: {n}",
                f"        description: \"{s['description']}\"",
            ]
        (self.out / "models" / "sources.yml").write_text("\n".join(lines))

    # ------------------------------------------------------------------
    # Seed DDL
    # ------------------------------------------------------------------
    def _write_seed_ddl(self, taskflows: list[Taskflow], mappings: list[Mapping]):
        col_ddl = ",\n    ".join(f"{c.lower()}  {d}" for c, d in self.ICS_OUTPUT_COLS.items())
        blocks = [
            "-- Auto-generated DuckDB DDL",
            "CREATE SCHEMA IF NOT EXISTS raw;\n",
        ]
        # Mapping source tables
        for m in mappings:
            for inst_name, inst in m.instances.items():
                t = m.transformations.get(inst.transformation_name)
                if t and t.ttype in ("Input Transformation", "Source Qualifier"):
                    tbl = t.attributes.get("Table Name", inst_name)
                    col_block = ",\n    ".join(
                        f"{f.name.lower()}  {f.duckdb_type}"
                        for f in t.outputs
                    ) or "id  integer"
                    blocks.append(
                        f"-- Source: {m.name} / {inst_name}\n"
                        f"CREATE TABLE IF NOT EXISTS raw.{_snake(tbl)} (\n    {col_block},\n    _loaded_at timestamp default current_timestamp\n);\n"
                    )
        # Taskflow audit tables
        for tf in taskflows:
            for step in tf.steps:
                tbl = f"raw.{_snake(step.task_name)}"
                blocks.append(
                    f"-- Task: {step.task_name} | GUID: {step.guid}\n"
                    f"CREATE TABLE IF NOT EXISTS {tbl} (\n    {col_ddl},\n    _loaded_at timestamp default current_timestamp\n);\n"
                )
        (self.out / "seeds" / "create_raw_tables.sql").write_text("\n".join(blocks))

    # ------------------------------------------------------------------
    # Lineage report
    # ------------------------------------------------------------------
    def _write_lineage_report(self, mappings: list[Mapping], taskflows: list[Taskflow], project_meta: Optional[dict]):
        lines = ["# Informatica → dbt Lineage Report\n"]
        if project_meta:
            lines += [
                f"**Project:** {project_meta['name']}",
                f"**Description:** {project_meta.get('description','')}",
                f"**Owner:** {project_meta.get('owner','')}",
                f"**Created:** {project_meta.get('created','')}",
                "",
            ]
        lines.append("## Mappings / Mapplets\n")
        for m in mappings:
            lines += [f"### `{m.name}` ({'Mapplet' if m.is_mapplet else 'Mapping'}) — folder: `{m.folder}`\n"]
            ordered = m.topo_sort()
            lines.append("**Transformation DAG (execution order):**\n")
            for i, inst_name in enumerate(ordered):
                inst = m.instances.get(inst_name)
                t = m.transformations.get(inst.transformation_name) if inst else None
                ttype = t.ttype if t else "?"
                arrow = " → " if i < len(ordered) - 1 else ""
                lines.append(f"  `{inst_name}` ({ttype}){arrow}")
            lines.append("\n**Connectors:**\n")
            for c in m.connectors:
                lines.append(f"  - `{c.from_instance}.{c.from_field}` → `{c.to_instance}.{c.to_field}`")
            lines.append("\n**Expressions:**\n")
            for tname, t in m.transformations.items():
                for f in t.fields:
                    if f.expression:
                        dbt_expr = ExpressionTranspiler().transpile(f.expression)
                        lines.append(f"  - `{tname}.{f.name}`: `{f.expression[:80]}` → `{dbt_expr[:80]}`")
            lines.append("")
        lines.append("## Taskflows\n")
        for tf in taskflows:
            lines += [f"### `{tf.name}`\n", f"{tf.description}\n", "**Steps:**\n"]
            for i, s in enumerate(tf.steps, 1):
                lines.append(f"  {i}. `{s.task_name}` ({s.task_type}) GUID:`{s.guid}` error:`{s.error_handler}`")
            lines.append("")
        (self.out / "lineage_report.md").write_text("\n".join(lines))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _snake(name: str) -> str:
    s = re.sub(r"[-\s.]+", "_", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower().strip("_")


def collect_files(path: str, exts: tuple) -> list[str]:
    p = Path(path)
    if p.is_file() and p.suffix.lower() in exts:
        return [str(p)]
    if p.is_dir():
        return [str(f) for f in sorted(p.rglob("*")) if f.suffix.lower() in exts]
    return []


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Convert Informatica XMLs + JSON → dbt/DuckDB")
    ap.add_argument("--xml",  required=True,
                    help="File or directory containing PowerMart XMLs and/or IICS Taskflow XMLs")
    ap.add_argument("--json", default=None,
                    help="IICS Project JSON export (optional)")
    ap.add_argument("--out",  default="./dbt_output",
                    help="Output directory (default: ./dbt_output)")
    args = ap.parse_args()

    xml_files = collect_files(args.xml, (".xml",))
    print(f"Found {len(xml_files)} XML file(s).\n")

    tf_parser  = InformaticaTaskflowParser()
    pm_parser  = PowerMartParser()

    taskflows: list[Taskflow] = []
    mappings:  list[Mapping]  = []

    for xf in xml_files:
        print(f"  Parsing: {xf}")
        # Try taskflow first
        try:
            tf = tf_parser.parse_file(xf)
            print(f"    → Taskflow '{tf.name}' ({len(tf.steps)} step(s))")
            taskflows.append(tf)
            continue
        except Exception:
            pass
        # Try PowerMart
        try:
            ms = pm_parser.parse_file(xf)
            for m in ms:
                kind = "Mapplet" if m.is_mapplet else "Mapping"
                print(f"    → {kind} '{m.name}' ({len(m.transformations)} transforms, {len(m.connectors)} connectors)")
            mappings.extend(ms)
        except Exception as e:
            print(f"    ⚠  Skipped ({e})")

    project_meta: Optional[dict] = None
    if args.json:
        try:
            project_meta = ProjectJsonParser().parse_file(args.json)
            print(f"\n  Project JSON: '{project_meta['name']}' — {project_meta['description']}")
        except Exception as e:
            print(f"  ⚠  Project JSON skipped ({e})")

    if not taskflows and not mappings:
        print("\nNothing parsed. Exiting.")
        return

    gen = DbtGenerator(out_dir=args.out)
    gen.generate(taskflows, mappings, project_meta)


if __name__ == "__main__":
    main()
