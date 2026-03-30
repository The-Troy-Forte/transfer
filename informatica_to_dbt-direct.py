"""
Informatica PowerCenter + IDMC -> dbt (DuckDB) Converter
=========================================================
Supports THREE source modes (mutually exclusive CLI flags):

  --xml   Local PowerMart/Taskflow XML files  (original behaviour)
  --idmc  Fetch live from IDMC REST API
  --pc    Fetch live from PowerCenter via:
            - Repository Web Services (SOAP)
            - Direct repository DB  (Oracle / SQL Server via SQLAlchemy)
            - Local XML export files (Repository Manager exports)

All three modes feed the same dbt generator, producing:
  models/staging/        SQL models per mapping / mapplet / session
  models/intermediate/   Audit roll-up models per workflow / taskflow
  models/marts/          (empty scaffold)
  models/schema.yml      Column-level tests
  models/sources.yml     Source declarations
  dbt_project.yml
  profiles.yml
  macros/                run_ics_task, infa_lookup, iif
  seeds/create_raw_tables.sql
  connections_catalog.json
  param_catalog.json     PowerCenter parameter files
  lineage_report.md

Dependencies (install what you need):
  pip install httpx python-dotenv dbt-duckdb   # always useful
  pip install sqlalchemy cx_Oracle              # PC direct-DB mode (Oracle)
  pip install sqlalchemy pyodbc                 # PC direct-DB mode (SQL Server)
  pip install requests lxml                     # PC SOAP mode

Usage examples:
  # Local files (XML exports)
  python informatica_to_dbt.py --xml ./exports/ --json project.json --out ./out

  # IDMC live
  python informatica_to_dbt.py --idmc --token <tok> --pod-url https://usw1.dm-us.informaticacloud.com

  # PowerCenter SOAP
  python informatica_to_dbt.py --pc --pc-mode soap \\
      --pc-host infa-repo.corp.com --pc-port 7333 \\
      --pc-user pmadmin --pc-password secret --pc-repo DEV_REPO

  # PowerCenter direct DB (Oracle)
  python informatica_to_dbt.py --pc --pc-mode db \\
      --pc-db-url "oracle+cx_oracle://user:pass@host:1521/SID"

  # PowerCenter XML exports folder
  python informatica_to_dbt.py --pc --pc-mode xml --xml ./pc_exports/
"""

import argparse
import json
import os
import re
import sys
import time
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

# Optional deps — loaded lazily
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import httpx
    _HTTP = "httpx"
except ImportError:
    import urllib.request
    _HTTP = "urllib"


# ===========================================================================
# Shared helpers
# ===========================================================================
def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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


# ===========================================================================
# PowerMart / IICS namespace maps
# ===========================================================================
NS = {
    "sf":   "http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd",
    "repo": "http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd",
}

INFA_TYPE_MAP: dict[str, str] = {
    "string": "varchar", "nstring": "varchar", "char": "varchar",
    "nchar": "varchar",  "text": "varchar",
    "double": "double",  "decimal": "decimal(38,10)",
    "integer": "integer","smallinteger": "smallint", "bigint": "bigint",
    "real": "float",     "float": "float",
    "date/time": "timestamp", "date": "date", "time": "time",
    "binary": "blob",
}


# ===========================================================================
# Core data classes
# ===========================================================================
@dataclass
class TransformField:
    name: str
    datatype: str
    port_type: str
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
    ttype: str
    fields: list[TransformField] = field(default_factory=list)
    attributes: dict[str, str] = field(default_factory=dict)
    description: str = ""
    reusable: bool = False

    @property
    def inputs(self):
        return [f for f in self.fields if f.is_input]

    @property
    def outputs(self):
        return [f for f in self.fields if f.is_output]

    @property
    def locals(self):
        return [f for f in self.fields if f.is_local]

    @property
    def lookup_table(self):
        return self.attributes.get("Lookup table name", "")

    @property
    def lookup_sql(self):
        return self.attributes.get("Lookup Sql Override", "")

    @property
    def lookup_condition(self):
        return self.attributes.get("Lookup condition", "")

    @property
    def filter_condition(self):
        return self.attributes.get("Filter Condition", "")

    @property
    def sp_name(self):
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


@dataclass
class SessionTask:
    """Represents a PowerCenter Session (s_*) within a Workflow."""
    name: str
    mapping_name: str
    folder: str
    source_connections: dict[str, str] = field(default_factory=dict)
    target_connections: dict[str, str] = field(default_factory=dict)
    parameters: dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class Workflow:
    """PowerCenter Workflow containing ordered sessions."""
    name: str
    folder: str
    description: str = ""
    sessions: list[SessionTask] = field(default_factory=list)
    parameters: dict[str, str] = field(default_factory=dict)


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


# ===========================================================================
# PowerMart XML Parser  (handles Mappings, Mapplets, Workflows, Sessions)
# ===========================================================================
class PowerMartParser:

    def parse_file(self, xml_path: str) -> tuple[list[Mapping], list[Workflow]]:
        """Returns (mappings, workflows) parsed from a PowerMart XML."""
        try:
            tree = ET.parse(xml_path)
        except ET.ParseError:
            # Some PC exports use Latin-1 — retry with explicit encoding
            with open(xml_path, encoding="latin-1") as fh:
                tree = ET.parse(fh)
        root = tree.getroot()
        mappings:  list[Mapping]  = []
        workflows: list[Workflow] = []

        for repo in root.iter("REPOSITORY"):
            for folder in repo.iter("FOLDER"):
                folder_name = folder.get("NAME", "")
                # Mapplets
                for mlt in folder.iter("MAPPLET"):
                    m = self._parse_mapping_el(mlt, folder_name, is_mapplet=True)
                    if m:
                        mappings.append(m)
                # Mappings
                for mapping in folder.findall("MAPPING"):
                    m = self._parse_mapping_el(mapping, folder_name, is_mapplet=False)
                    if m:
                        mappings.append(m)
                # Workflows
                for wf in folder.findall("WORKFLOW"):
                    w = self._parse_workflow(wf, folder_name)
                    if w:
                        workflows.append(w)
        return mappings, workflows

    # ------------------------------------------------------------------
    def _parse_mapping_el(self, el: ET.Element, folder: str, is_mapplet: bool) -> Optional[Mapping]:
        m = Mapping(
            name=el.get("NAME", ""),
            folder=folder,
            description=el.get("DESCRIPTION", ""),
            is_mapplet=is_mapplet,
        )
        for t in el.findall("TRANSFORMATION"):
            trans = self._parse_transformation(t)
            m.transformations[trans.name] = trans
        for inst in el.findall("INSTANCE"):
            i = MappingInstance(
                name=inst.get("NAME", ""),
                transformation_name=inst.get("TRANSFORMATION_NAME", inst.get("NAME", "")),
                transformation_type=inst.get("TRANSFORMATION_TYPE", ""),
                reusable=inst.get("REUSABLE", "NO").upper() == "YES",
            )
            m.instances[i.name] = i
        if not m.instances:
            for tname, trans in m.transformations.items():
                m.instances[tname] = MappingInstance(tname, tname, trans.ttype)
        for c in el.findall("CONNECTOR"):
            m.connectors.append(Connector(
                from_instance=c.get("FROMINSTANCE", ""),
                from_field=c.get("FROMFIELD", ""),
                to_instance=c.get("TOINSTANCE", ""),
                to_field=c.get("TOFIELD", ""),
            ))
        return m if (m.transformations or m.connectors) else None

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

    # ------------------------------------------------------------------
    def _parse_workflow(self, el: ET.Element, folder: str) -> Optional[Workflow]:
        wf = Workflow(
            name=el.get("NAME", ""),
            folder=folder,
            description=el.get("DESCRIPTION", ""),
        )
        # Workflow-level parameters
        for p in el.findall("TASKINSTANCE/ATTRIBUTE"):
            wf.parameters[p.get("NAME", "")] = p.get("VALUE", "")

        # Walk WORKLET and SESSION task instances
        for task in el.iter("TASKINSTANCE"):
            task_type = task.get("TASKTYPE", "")
            if task_type not in ("SESSION", "WORKLET"):
                continue
            sess = self._parse_session(task, folder)
            if sess:
                wf.sessions.append(sess)
        # Also look for SESSION elements directly nested
        for sess_el in el.findall("SESSION"):
            sess = self._parse_session_el(sess_el, folder)
            if sess:
                wf.sessions.append(sess)
        return wf if wf.sessions else None

    def _parse_session(self, task_el: ET.Element, folder: str) -> Optional[SessionTask]:
        name    = task_el.get("NAME", task_el.get("TASKNAME", ""))
        mapping = ""
        src_conns, tgt_conns, params = {}, {}, {}
        for attr in task_el.findall("ATTRIBUTE"):
            aname = attr.get("NAME", "")
            aval  = attr.get("VALUE", "")
            if aname == "Mapping name":
                mapping = aval
            elif "source" in aname.lower() and "connection" in aname.lower():
                src_conns[aname] = aval
            elif "target" in aname.lower() and "connection" in aname.lower():
                tgt_conns[aname] = aval
            else:
                params[aname] = aval
        return SessionTask(name=name, mapping_name=mapping, folder=folder,
                           source_connections=src_conns,
                           target_connections=tgt_conns, parameters=params)

    def _parse_session_el(self, el: ET.Element, folder: str) -> Optional[SessionTask]:
        name    = el.get("NAME", "")
        mapping = el.get("MAPPINGNAME", "")
        src_conns, tgt_conns, params = {}, {}, {}
        for attr in el.findall(".//ATTRIBUTE"):
            aname = attr.get("NAME", "")
            aval  = attr.get("VALUE", "")
            if "source" in aname.lower() and "connection" in aname.lower():
                src_conns[aname] = aval
            elif "target" in aname.lower() and "connection" in aname.lower():
                tgt_conns[aname] = aval
            else:
                params[aname] = aval
        return SessionTask(name=name, mapping_name=mapping, folder=folder,
                           source_connections=src_conns,
                           target_connections=tgt_conns, parameters=params)


# ===========================================================================
# Parameter File Parser  (.par / .properties)
# ===========================================================================
class ParameterFileParser:
    """
    Parses PowerCenter parameter files.

    Format:
      [folder.workflow.session]
      $$PARAM_NAME=value
      $DBConnection_SomeConn=ConnName
    """

    def parse_file(self, path: str) -> dict[str, dict[str, str]]:
        """Returns {section: {param: value}}."""
        catalog: dict[str, dict[str, str]] = {}
        current: dict[str, str] = {}
        section = "__global__"
        with open(path, encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("[") and line.endswith("]"):
                    catalog[section] = current
                    section = line[1:-1]
                    current = catalog.setdefault(section, {})
                elif "=" in line:
                    k, _, v = line.partition("=")
                    current[k.strip()] = v.strip()
        catalog[section] = current
        return catalog

    def parse_dir(self, path: str) -> dict[str, dict[str, str]]:
        all_params: dict[str, dict[str, str]] = {}
        for f in collect_files(path, (".par", ".properties", ".param")):
            all_params.update(self.parse_file(f))
        return all_params


# ===========================================================================
# PowerCenter Repository Web Services (SOAP) Client
# ===========================================================================
class PCSOAPClient:
    """
    Minimal SOAP client for the PowerCenter Repository Web Service.
    Tested against PCWS 9.x / 10.x.

    Endpoints used:
      - Login        -> session token
      - GetAllFolders
      - GetAllMappings (per folder)
      - GetAllWorkflows (per folder)
      - ReadObject   -> full object XML
    """

    _WSDL_PATH = "/wsdl/Metadata"    # adjust to your installation

    def __init__(self, host: str, port: int, repo: str, user: str, password: str,
                 domain: str = "Native", use_https: bool = False):
        self.base  = f"{'https' if use_https else 'http'}://{host}:{port}"
        self.repo  = repo
        self.user  = user
        self.password = password
        self.domain = domain
        self._token: str = ""
        self._session: object = None   # requests.Session

    # ------------------------------------------------------------------
    def connect(self):
        try:
            import requests
            self._session = requests.Session()
        except ImportError:
            raise RuntimeError("Install 'requests' for SOAP mode: pip install requests lxml")

        self._token = self._login()
        print(f"  [PC SOAP] Connected to {self.base}, repo={self.repo}")

    def _soap_envelope(self, body_xml: str) -> str:
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:mws="http://www.informatica.com/wsh">'
            "<soapenv:Header/>"
            f"<soapenv:Body>{body_xml}</soapenv:Body>"
            "</soapenv:Envelope>"
        )

    def _post(self, action: str, body_xml: str) -> ET.Element:
        url = f"{self.base}/wsh/services/RepositoryService"
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction":   f'"{action}"',
        }
        if self._token:
            headers["Authorization"] = f"Basic {self._token}"
        payload = self._soap_envelope(body_xml)
        resp = self._session.post(url, data=payload.encode(), headers=headers, timeout=120)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        # Strip SOAP envelope
        body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
        return body[0] if body is not None and len(body) else root

    def _login(self) -> str:
        body = (
            f"<mws:Login>"
            f"<mws:RepositoryDomainName>{self.domain}</mws:RepositoryDomainName>"
            f"<mws:RepositoryName>{self.repo}</mws:RepositoryName>"
            f"<mws:UserName>{self.user}</mws:UserName>"
            f"<mws:Password>{self.password}</mws:Password>"
            f"</mws:Login>"
        )
        resp = self._post("Login", body)
        token_el = resp.find(".//{*}SessionToken")
        return token_el.text if token_el is not None else ""

    # ------------------------------------------------------------------
    def list_folders(self) -> list[str]:
        body = f"<mws:GetAllFolders><mws:SessionToken>{self._token}</mws:SessionToken></mws:GetAllFolders>"
        resp = self._post("GetAllFolders", body)
        return [el.text for el in resp.findall(".//{*}FolderName") if el.text]

    def export_folder_xml(self, folder: str) -> bytes:
        """Export an entire folder as PowerMart XML."""
        body = (
            f"<mws:ExportObjects>"
            f"<mws:SessionToken>{self._token}</mws:SessionToken>"
            f"<mws:ObjectsToExport>"
            f"  <mws:FolderName>{folder}</mws:FolderName>"
            f"  <mws:ObjectTypes>mapping</mws:ObjectTypes>"
            f"  <mws:ObjectTypes>mapplet</mws:ObjectTypes>"
            f"  <mws:ObjectTypes>workflow</mws:ObjectTypes>"
            f"  <mws:ObjectTypes>session</mws:ObjectTypes>"
            f"  <mws:ObjectTypes>source</mws:ObjectTypes>"
            f"  <mws:ObjectTypes>target</mws:ObjectTypes>"
            f"</mws:ObjectsToExport>"
            f"<mws:DependencyOptions>"
            f"  <mws:AddDependency>false</mws:AddDependency>"
            f"</mws:DependencyOptions>"
            f"</mws:ExportObjects>"
        )
        resp = self._post("ExportObjects", body)
        xml_el = resp.find(".//{*}ExportedObjects")
        if xml_el is not None and xml_el.text:
            return xml_el.text.encode()
        # Return the whole response serialised as bytes
        return ET.tostring(resp)

    def disconnect(self):
        if self._token:
            try:
                body = f"<mws:Logout><mws:SessionToken>{self._token}</mws:SessionToken></mws:Logout>"
                self._post("Logout", body)
            except Exception:
                pass


# ===========================================================================
# PowerCenter Direct-DB Reader (Oracle / SQL Server via SQLAlchemy)
# ===========================================================================
class PCDirectDBReader:
    """
    Reads PowerCenter repository metadata directly from the backend DB.
    Works with Oracle and SQL Server repositories.

    Key tables used:
      OPB_MAPPING, OPB_WIDGET, OPB_WIDGET_FIELD, OPB_LINK,
      OPB_SESSION, OPB_TASK, OPB_WFLOW_RUN (workflows),
      OPB_CNX (connections), REP_SUBJECT (folders)
    """

    # SQL fragments — compatible with Oracle and SQL Server
    _SQL_FOLDERS = "SELECT SUBJ_ID, SUBJ_NAME FROM OPB_SUBJECT ORDER BY SUBJ_NAME"

    _SQL_MAPPINGS = """
        SELECT m.MAPPING_ID, m.MAPPING_NAME, m.COMMENTS,
               s.SUBJ_NAME AS FOLDER_NAME,
               CASE WHEN m.IS_MAPPLET = 1 THEN 'MAPPLET' ELSE 'MAPPING' END AS OBJ_TYPE
        FROM OPB_MAPPING m
        JOIN OPB_SUBJECT s ON s.SUBJ_ID = m.SUBJECT_ID
        ORDER BY s.SUBJ_NAME, m.MAPPING_NAME
    """

    _SQL_WIDGETS = """
        SELECT w.WIDGET_ID, w.WIDGET_NAME, w.WIDGET_TYPE_NAME,
               w.MAPPING_ID, w.REUSABLE, w.DESCRIPTION
        FROM OPB_WIDGET w
        WHERE w.MAPPING_ID = :mapping_id
        ORDER BY w.WIDGET_ID
    """

    _SQL_WIDGET_FIELDS = """
        SELECT f.WIDGET_ID, f.FIELD_NAME, f.DATATYPE,
               f.PORTTYPE, f.DEFAULT_VALUE, f.DESCRIPTION,
               f.PRECISION, f.SCALE, f.EXPRESSION
        FROM OPB_WIDGET_FIELD f
        WHERE f.WIDGET_ID IN (
            SELECT WIDGET_ID FROM OPB_WIDGET WHERE MAPPING_ID = :mapping_id
        )
        ORDER BY f.WIDGET_ID, f.FIELD_ID
    """

    _SQL_LINKS = """
        SELECT l.FROM_WIDGET_ID, l.FROM_FIELD_NAME,
               l.TO_WIDGET_ID,   l.TO_FIELD_NAME
        FROM OPB_LINK l
        WHERE l.MAPPING_ID = :mapping_id
    """

    _SQL_WIDGET_ATTRS = """
        SELECT a.WIDGET_ID, a.ATTR_NAME, a.ATTR_VALUE
        FROM OPB_WIDGET_ATTR a
        WHERE a.WIDGET_ID IN (
            SELECT WIDGET_ID FROM OPB_WIDGET WHERE MAPPING_ID = :mapping_id
        )
    """

    _SQL_WORKFLOWS = """
        SELECT t.TASK_ID, t.TASK_NAME, t.COMMENTS,
               s.SUBJ_NAME AS FOLDER_NAME
        FROM OPB_TASK t
        JOIN OPB_SUBJECT s ON s.SUBJ_ID = t.SUBJECT_ID
        WHERE t.TASK_TYPE = 71   -- 71 = Workflow
        ORDER BY s.SUBJ_NAME, t.TASK_NAME
    """

    _SQL_SESSIONS = """
        SELECT ti.INSTANCE_NAME, ti.MAPPING_NAME,
               ti.TASK_ID AS WF_TASK_ID
        FROM OPB_TASK_INST ti
        WHERE ti.WORKFLOW_ID = :wf_id
          AND ti.TASK_TYPE = 68   -- 68 = Session
    """

    _SQL_CONNECTIONS = """
        SELECT c.OBJECT_ID, c.OBJECT_NAME, c.OBJECT_TYPE_NAME,
               c.USER_NAME, c.SERVER_NAME, c.PORT_NO, c.DATABASE_NAME
        FROM OPB_CNX c
        ORDER BY c.OBJECT_NAME
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        self._engine = None

    def connect(self):
        try:
            from sqlalchemy import create_engine, text
            self._engine = create_engine(self.db_url, echo=False)
            self._text = text
            with self._engine.connect() as conn:
                conn.execute(self._text("SELECT 1 FROM DUAL"))
            print(f"  [PC DB] Connected: {self.db_url.split('@')[-1]}")
        except ImportError:
            raise RuntimeError(
                "Install SQLAlchemy + DB driver:\n"
                "  Oracle : pip install sqlalchemy cx_Oracle\n"
                "  SQL Svr: pip install sqlalchemy pyodbc"
            )

    def _query(self, sql: str, params: Optional[dict] = None) -> list[dict]:
        with self._engine.connect() as conn:
            result = conn.execute(self._text(sql), params or {})
            cols = list(result.keys())
            return [dict(zip(cols, row)) for row in result]

    # ------------------------------------------------------------------
    def fetch_all(self) -> tuple[list[Mapping], list[Workflow], list[dict]]:
        print("  [PC DB] Reading mappings...")
        mappings  = self._fetch_mappings()
        print(f"    -> {len(mappings)} mapping(s) / mapplet(s)")
        print("  [PC DB] Reading workflows...")
        workflows = self._fetch_workflows()
        print(f"    -> {len(workflows)} workflow(s)")
        print("  [PC DB] Reading connections...")
        connections = self._fetch_connections()
        print(f"    -> {len(connections)} connection(s)")
        return mappings, workflows, connections

    def _fetch_mappings(self) -> list[Mapping]:
        rows = self._query(self._SQL_MAPPINGS)
        mappings = []
        for row in rows:
            mid = row["MAPPING_ID"]
            m = Mapping(
                name=row["MAPPING_NAME"],
                folder=row["FOLDER_NAME"],
                description=row.get("COMMENTS", ""),
                is_mapplet=(row["OBJ_TYPE"] == "MAPPLET"),
            )
            # Widgets -> Transformations
            widget_rows = self._query(self._SQL_WIDGETS, {"mapping_id": mid})
            widget_map: dict[int, str] = {}   # id -> name
            for wr in widget_rows:
                wid  = wr["WIDGET_ID"]
                wname = wr["WIDGET_NAME"]
                widget_map[wid] = wname
                t = Transformation(
                    name=wname,
                    ttype=wr.get("WIDGET_TYPE_NAME", ""),
                    description=wr.get("DESCRIPTION", ""),
                    reusable=bool(wr.get("REUSABLE", 0)),
                )
                m.transformations[wname] = t
                m.instances[wname] = MappingInstance(wname, wname, t.ttype)

            # Fields
            field_rows = self._query(self._SQL_WIDGET_FIELDS, {"mapping_id": mid})
            for fr in field_rows:
                wname = widget_map.get(fr["WIDGET_ID"], "")
                if wname not in m.transformations:
                    continue
                m.transformations[wname].fields.append(TransformField(
                    name=fr["FIELD_NAME"],
                    datatype=fr.get("DATATYPE", "string"),
                    port_type=fr.get("PORTTYPE", "INPUT"),
                    expression=fr.get("EXPRESSION", "") or "",
                    precision=int(fr.get("PRECISION", 4000) or 4000),
                    scale=int(fr.get("SCALE", 0) or 0),
                    default_value=fr.get("DEFAULT_VALUE", "") or "",
                    description=fr.get("DESCRIPTION", "") or "",
                ))

            # Attributes
            attr_rows = self._query(self._SQL_WIDGET_ATTRS, {"mapping_id": mid})
            for ar in attr_rows:
                wname = widget_map.get(ar["WIDGET_ID"], "")
                if wname in m.transformations:
                    m.transformations[wname].attributes[ar["ATTR_NAME"]] = ar["ATTR_VALUE"] or ""

            # Links -> Connectors
            link_rows = self._query(self._SQL_LINKS, {"mapping_id": mid})
            for lr in link_rows:
                m.connectors.append(Connector(
                    from_instance=widget_map.get(lr["FROM_WIDGET_ID"], str(lr["FROM_WIDGET_ID"])),
                    from_field=lr["FROM_FIELD_NAME"],
                    to_instance=widget_map.get(lr["TO_WIDGET_ID"],   str(lr["TO_WIDGET_ID"])),
                    to_field=lr["TO_FIELD_NAME"],
                ))

            if m.transformations:
                mappings.append(m)
        return mappings

    def _fetch_workflows(self) -> list[Workflow]:
        rows = self._query(self._SQL_WORKFLOWS)
        workflows = []
        for row in rows:
            wf = Workflow(
                name=row["TASK_NAME"],
                folder=row["FOLDER_NAME"],
                description=row.get("COMMENTS", ""),
            )
            sess_rows = self._query(self._SQL_SESSIONS, {"wf_id": row["TASK_ID"]})
            for sr in sess_rows:
                wf.sessions.append(SessionTask(
                    name=sr["INSTANCE_NAME"],
                    mapping_name=sr.get("MAPPING_NAME", ""),
                    folder=row["FOLDER_NAME"],
                ))
            if wf.sessions:
                workflows.append(wf)
        return workflows

    def _fetch_connections(self) -> list[dict]:
        rows = self._query(self._SQL_CONNECTIONS)
        return [
            {
                "id":       str(r.get("OBJECT_ID", "")),
                "name":     r.get("OBJECT_NAME", ""),
                "type":     r.get("OBJECT_TYPE_NAME", ""),
                "host":     r.get("SERVER_NAME", ""),
                "port":     str(r.get("PORT_NO", "")),
                "database": r.get("DATABASE_NAME", ""),
                "username": r.get("USER_NAME", ""),
            }
            for r in rows
        ]


# ===========================================================================
# PowerCenter Fetcher  (SOAP or DB, normalises to same object model)
# ===========================================================================
class PCFetcher:
    """
    High-level fetcher that wraps either the SOAP or DB client and
    returns (mappings, workflows, connections, params) in the standard model.
    """

    def __init__(self, args, out_dir: Path):
        self.args    = args
        self.out     = out_dir
        self.raw_dir = out_dir / "_pc_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.pm_parser  = PowerMartParser()
        self.par_parser = ParameterFileParser()

    # ------------------------------------------------------------------
    def fetch_all(self) -> tuple[list[Mapping], list[Workflow], list[dict], dict]:
        mode = getattr(self.args, "pc_mode", "xml")

        if mode == "soap":
            return self._fetch_via_soap()
        elif mode == "db":
            return self._fetch_via_db()
        else:
            return self._fetch_via_xml()

    # ------------------------------------------------------------------
    def _fetch_via_soap(self) -> tuple[list[Mapping], list[Workflow], list[dict], dict]:
        client = PCSOAPClient(
            host=self.args.pc_host,
            port=int(self.args.pc_port or 7333),
            repo=self.args.pc_repo,
            user=self.args.pc_user,
            password=self.args.pc_password,
            use_https=getattr(self.args, "pc_https", False),
        )
        client.connect()

        all_mappings:  list[Mapping]  = []
        all_workflows: list[Workflow] = []

        try:
            folders = client.list_folders()
            print(f"  [PC SOAP] Found {len(folders)} folder(s)")
            for folder in folders:
                print(f"    Exporting folder: {folder}")
                try:
                    xml_bytes = client.export_folder_xml(folder)
                    xml_path  = self.raw_dir / f"{_snake(folder)}.xml"
                    xml_path.write_bytes(xml_bytes)
                    ms, wfs = self.pm_parser.parse_file(str(xml_path))
                    all_mappings.extend(ms)
                    all_workflows.extend(wfs)
                    print(f"      -> {len(ms)} mapping(s), {len(wfs)} workflow(s)")
                except Exception as e:
                    print(f"      Warning: folder '{folder}' failed: {e}")
        finally:
            client.disconnect()

        params = self._load_param_files()
        connections = self._connections_from_sessions(all_workflows)
        return all_mappings, all_workflows, connections, params

    # ------------------------------------------------------------------
    def _fetch_via_db(self) -> tuple[list[Mapping], list[Workflow], list[dict], dict]:
        reader = PCDirectDBReader(self.args.pc_db_url)
        reader.connect()
        mappings, workflows, connections = reader.fetch_all()

        # Save raw snapshot
        _write(
            self.raw_dir / "db_mappings.json",
            json.dumps([{"name": m.name, "folder": m.folder,
                         "transforms": len(m.transformations),
                         "connectors": len(m.connectors)}
                        for m in mappings], indent=2),
        )
        params = self._load_param_files()
        return mappings, workflows, connections, params

    # ------------------------------------------------------------------
    def _fetch_via_xml(self) -> tuple[list[Mapping], list[Workflow], list[dict], dict]:
        xml_path = getattr(self.args, "xml", None) or getattr(self.args, "pc_xml_dir", ".")
        files    = collect_files(xml_path, (".xml",))
        print(f"  [PC XML] Found {len(files)} XML file(s)")
        all_mappings:  list[Mapping]  = []
        all_workflows: list[Workflow] = []
        for f in files:
            print(f"    Parsing: {f}")
            try:
                ms, wfs = self.pm_parser.parse_file(f)
                all_mappings.extend(ms)
                all_workflows.extend(wfs)
                print(f"      -> {len(ms)} mapping(s), {len(wfs)} workflow(s)")
            except Exception as e:
                print(f"      Warning: {e}")
        params = self._load_param_files()
        connections = self._connections_from_sessions(all_workflows)
        return all_mappings, all_workflows, connections, params

    # ------------------------------------------------------------------
    def _load_param_files(self) -> dict:
        par_path = getattr(self.args, "pc_params", None)
        if par_path:
            try:
                params = self.par_parser.parse_dir(par_path) \
                         if Path(par_path).is_dir() \
                         else self.par_parser.parse_file(par_path)
                _write(
                    self.out / "param_catalog.json",
                    json.dumps(params, indent=2),
                )
                print(f"  [PC] Loaded {len(params)} parameter section(s) -> param_catalog.json")
                return params
            except Exception as e:
                print(f"  Warning: param file load failed: {e}")
        return {}

    @staticmethod
    def _connections_from_sessions(workflows: list[Workflow]) -> list[dict]:
        seen: set[str] = set()
        conns = []
        for wf in workflows:
            for s in wf.sessions:
                for name in list(s.source_connections.values()) + list(s.target_connections.values()):
                    if name and name not in seen:
                        seen.add(name)
                        conns.append({"id": name, "name": name,
                                      "type": "inferred", "host": "", "database": ""})
        return conns


# ===========================================================================
# IICS Taskflow Parser
# ===========================================================================
class InformaticaTaskflowParser:

    def parse_file(self, xml_path: str) -> Taskflow:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        entry   = root.find(".//{%s}Entry" % NS["repo"])
        tf_root = entry.find("{%s}taskflow" % NS["sf"]) if entry is not None else None
        if tf_root is None:
            tf_root = root.find(".//{%s}taskflow" % NS["sf"])
        if tf_root is None:
            raise ValueError(f"No <taskflow> element in {xml_path}")

        name    = tf_root.get("name", Path(xml_path).stem)
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
        lnk = el.find("{%s}link" % NS["sf"])
        return lnk.get("targetId") if lnk is not None else None

    def _parse_event_container(self, ec_id, ec) -> Optional[MappingStep]:
        svc = ec.find("{%s}service" % NS["sf"])
        if svc is None:
            return None
        title  = (svc.findtext("{%s}title" % NS["sf"]) or "").strip()
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


# ===========================================================================
# Project JSON Parser
# ===========================================================================
class ProjectJsonParser:
    def parse_file(self, json_path: str) -> dict:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        props = {p["name"]: p["value"] for p in data.get("properties", []) if "name" in p}
        return {
            "id":          props.get("id", ""),
            "name":        props.get("name", ""),
            "description": props.get("description", ""),
            "owner":       props.get("owner", ""),
            "created":     props.get("createdTime", ""),
            "modified":    props.get("lastUpdatedTime", ""),
            "state":       props.get("documentState", ""),
        }


# ===========================================================================
# Expression Transpiler
# ===========================================================================
class ExpressionTranspiler:
    def transpile(self, expr: str) -> str:
        if not expr:
            return ""
        out = expr.replace("&apos;", "'").replace("&amp;", "&").replace("&#xD;&#xA;", "\n")
        out = re.sub(r":SP\.", "", out)
        out = self._iif_to_case(out)
        return out.strip()

    @staticmethod
    def _iif_to_case(expr: str) -> str:
        pattern = re.compile(r"\bIIF\s*\(", re.IGNORECASE)
        result, i = [], 0
        while i < len(expr):
            m = pattern.search(expr, i)
            if not m:
                result.append(expr[i:])
                break
            result.append(expr[i:m.start()])
            depth, j, buf = 1, m.end(), []
            while j < len(expr) and depth > 0:
                ch = expr[j]
                if ch == "(": depth += 1
                elif ch == ")": depth -= 1
                if depth > 0: buf.append(ch)
                j += 1
            args_str = "".join(buf)
            args = ExpressionTranspiler._split_args(args_str)
            if len(args) >= 3:
                result.append(
                    f"CASE WHEN {args[0].strip()} THEN {args[1].strip()} "
                    f"ELSE {args[2].strip()} END"
                )
            else:
                result.append(f"IIF({args_str})")
            i = j
        return "".join(result)

    @staticmethod
    def _split_args(s: str) -> list[str]:
        args, depth, buf = [], 0, []
        for ch in s:
            if ch == "(": depth += 1
            elif ch == ")": depth -= 1
            if ch == "," and depth == 0:
                args.append("".join(buf)); buf = []
            else:
                buf.append(ch)
        if buf: args.append("".join(buf))
        return args


# ===========================================================================
# IDMC REST Client  (unchanged)
# ===========================================================================
@dataclass
class IDMCConfig:
    pod_url: str
    token: str
    page_size: int = 50
    max_retries: int = 3
    retry_delay: float = 2.0

    @classmethod
    def from_args(cls, args) -> "IDMCConfig":
        pod_url = getattr(args, "pod_url", None) or os.environ.get("IDMC_POD_URL", "")
        token   = getattr(args, "token",   None) or os.environ.get("IDMC_TOKEN",   "")
        if not pod_url or not token:
            raise ValueError("Provide --pod-url and --token, or set IDMC_POD_URL / IDMC_TOKEN.")
        return cls(pod_url=pod_url.rstrip("/"), token=token)


class IDMCClient:
    _DESIGN_BASE   = "/api/v3"
    _PLATFORM_BASE = "/api/v2"

    def __init__(self, cfg: IDMCConfig):
        self.cfg = cfg
        self._headers = {
            "Authorization": f"Bearer {cfg.token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

    def _get(self, url: str, params: Optional[dict] = None) -> dict | list:
        full = url if url.startswith("http") else f"{self.cfg.pod_url}{url}"
        if params:
            full += "?" + "&".join(f"{k}={v}" for k, v in params.items())
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                if _HTTP == "httpx":
                    r = httpx.get(full, headers=self._headers, timeout=60)
                    r.raise_for_status(); return r.json()
                else:
                    req = urllib.request.Request(full, headers=self._headers)
                    with urllib.request.urlopen(req, timeout=60) as r:
                        return json.loads(r.read().decode())
            except Exception as exc:
                if attempt == self.cfg.max_retries:
                    raise RuntimeError(f"GET {full} failed: {exc}") from exc
                time.sleep(self.cfg.retry_delay * attempt)

    def _get_binary(self, url: str) -> bytes:
        full = url if url.startswith("http") else f"{self.cfg.pod_url}{url}"
        hdrs = dict(self._headers); hdrs["Accept"] = "application/xml, text/xml, */*"
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                if _HTTP == "httpx":
                    r = httpx.get(full, headers=hdrs, timeout=120)
                    r.raise_for_status(); return r.content
                else:
                    req = urllib.request.Request(full, headers=hdrs)
                    with urllib.request.urlopen(req, timeout=120) as r:
                        return r.read()
            except Exception as exc:
                if attempt == self.cfg.max_retries:
                    raise RuntimeError(f"Binary GET {full} failed: {exc}") from exc
                time.sleep(self.cfg.retry_delay * attempt)

    def _paginate(self, base: str, extra: Optional[dict] = None) -> list[dict]:
        results, offset = [], 0
        while True:
            params = {"limit": self.cfg.page_size, "skip": offset}
            if extra: params.update(extra)
            page  = self._get(base, params)
            items = page if isinstance(page, list) else page.get("items", page.get("data", []))
            if not items: break
            results.extend(items)
            if len(items) < self.cfg.page_size: break
            offset += self.cfg.page_size
        return results

    def list_mappings(self, project_id=None) -> list[dict]:
        p = {"type": "MTT"}
        if project_id: p["projectId"] = project_id
        items = self._paginate(f"{self._DESIGN_BASE}/mttasks", p)
        print(f"    IDMC: {len(items)} mapping task(s)")
        return items

    def list_mapplets(self, project_id=None) -> list[dict]:
        p = {"projectId": project_id} if project_id else {}
        items = self._paginate(f"{self._DESIGN_BASE}/mapplets", p)
        print(f"    IDMC: {len(items)} mapplet(s)")
        return items

    def export_mapping_xml(self, obj_id: str, obj_type: str = "MTT") -> bytes:
        payload = json.dumps({"objects": [{"id": obj_id, "type": obj_type}],
                               "options": {"exportFormat": "POWERMART"}}).encode()
        url  = f"{self.cfg.pod_url}{self._DESIGN_BASE}/export"
        hdrs = dict(self._headers); hdrs["Accept"] = "application/xml"
        if _HTTP == "httpx":
            r = httpx.post(url, content=payload, headers=hdrs, timeout=120)
            r.raise_for_status()
            if r.status_code == 202:
                return self._poll_export(r.headers.get("Location", ""))
            return r.content
        else:
            req = urllib.request.Request(url, data=payload, headers=hdrs, method="POST")
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read()

    def _poll_export(self, job_url: str, max_wait: int = 300) -> bytes:
        for _ in range(max_wait // 5):
            time.sleep(5)
            d = self._get(job_url)
            if d.get("status") == "SUCCESSFUL":
                return self._get_binary(d.get("downloadUrl", ""))
            if d.get("status") in ("FAILED", "ERROR"):
                raise RuntimeError(f"Export failed: {d}")
        raise TimeoutError("Export timed out")

    def get_mapping_detail(self, mid: str) -> dict:
        return self._get(f"{self._DESIGN_BASE}/mttasks/{mid}")

    def list_taskflows(self, project_id=None) -> list[dict]:
        p = {"projectId": project_id} if project_id else {}
        items = self._paginate(f"{self._DESIGN_BASE}/taskflows", p)
        print(f"    IDMC: {len(items)} taskflow(s)")
        return items

    def export_taskflow_xml(self, tf_id: str) -> bytes:
        return self._get_binary(f"{self._DESIGN_BASE}/taskflows/{tf_id}/export")

    def get_taskflow_detail(self, tf_id: str) -> dict:
        return self._get(f"{self._DESIGN_BASE}/taskflows/{tf_id}")

    def list_connections(self) -> list[dict]:
        items = self._paginate(f"{self._PLATFORM_BASE}/connections")
        print(f"    IDMC: {len(items)} connection(s)")
        return items

    def get_connection_detail(self, cid: str) -> dict:
        return self._get(f"{self._PLATFORM_BASE}/connections/{cid}")


class IDMCFetcher:
    def __init__(self, client: IDMCClient, out_dir: Path):
        self.client  = client
        self.out     = out_dir
        self.raw_dir = out_dir / "_idmc_raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def fetch_all(self, project_filter=None) -> tuple[list[Mapping], list[Taskflow], list[dict]]:
        print("\n[IDMC] Fetching connections...")
        conns = self._fetch_connections()
        print("\n[IDMC] Fetching mappings & mapplets...")
        mappings = self._fetch_mappings(project_filter)
        print("\n[IDMC] Fetching taskflows...")
        taskflows = self._fetch_taskflows(project_filter)
        return mappings, taskflows, conns

    def _fetch_connections(self) -> list[dict]:
        raw, catalog = self.client.list_connections(), []
        for c in raw:
            cid   = c.get("id", "")
            cname = c.get("name", cid)
            try:
                detail = self.client.get_connection_detail(cid)
            except Exception:
                detail = c
            entry = {"id": cid, "name": cname,
                     "type": detail.get("type", ""),
                     "host": detail.get("host", ""),
                     "port": str(detail.get("port", "")),
                     "database": detail.get("database", ""),
                     "username": detail.get("username", ""),
                     "raw": detail}
            catalog.append(entry)
            _write(self.raw_dir / "connections" / f"{_snake(cname)}.json",
                   json.dumps(detail, indent=2))
        _write(self.out / "connections_catalog.json", json.dumps(catalog, indent=2))
        print(f"    Saved connections_catalog.json ({len(catalog)})")
        return catalog

    def _fetch_mappings(self, project_filter) -> list[Mapping]:
        pm = PowerMartParser()
        all_m: list[Mapping] = []
        for item in self.client.list_mappings(project_filter):
            oid, oname = item.get("id",""), item.get("name","")
            print(f"    Fetching mapping: {oname}")
            try:
                xb = self.client.export_mapping_xml(oid, "MTT")
                xp = self.raw_dir / "mappings" / f"{_snake(oname)}.xml"
                xp.parent.mkdir(parents=True, exist_ok=True)
                xp.write_bytes(xb)
                ms, _ = pm.parse_file(str(xp))
                all_m.extend(ms)
                print(f"      -> {len(ms)} mapping(s)")
            except Exception as e:
                print(f"      Warning: {e}")
        for item in self.client.list_mapplets(project_filter):
            oid, oname = item.get("id",""), item.get("name","")
            print(f"    Fetching mapplet: {oname}")
            try:
                xb = self.client.export_mapping_xml(oid, "MAPPLET")
                xp = self.raw_dir / "mapplets" / f"{_snake(oname)}.xml"
                xp.parent.mkdir(parents=True, exist_ok=True)
                xp.write_bytes(xb)
                ms, _ = pm.parse_file(str(xp))
                all_m.extend(ms)
            except Exception as e:
                print(f"      Warning: {e}")
        return all_m

    def _fetch_taskflows(self, project_filter) -> list[Taskflow]:
        tfp = InformaticaTaskflowParser()
        tfs: list[Taskflow] = []
        for item in self.client.list_taskflows(project_filter):
            tid, tname = item.get("id",""), item.get("name","")
            print(f"    Fetching taskflow: {tname}")
            try:
                xb = self.client.export_taskflow_xml(tid)
                xp = self.raw_dir / "taskflows" / f"{_snake(tname)}.xml"
                xp.parent.mkdir(parents=True, exist_ok=True)
                xp.write_bytes(xb)
                tf = tfp.parse_file(str(xp))
                tfs.append(tf)
                print(f"      -> '{tf.name}' ({len(tf.steps)} step(s))")
            except Exception as e:
                print(f"      Warning: {e}")
        return tfs


# ===========================================================================
# dbt / DuckDB Generator
# ===========================================================================
class DbtGenerator:

    ICS_OUTPUT_COLS: dict[str, str] = {
        "Object_Name": "varchar",    "Run_Id": "bigint",
        "Log_Id": "bigint",          "Task_Id": "varchar",
        "Task_Status": "varchar",    "Success_Source_Rows": "bigint",
        "Failed_Source_Rows": "bigint", "Success_Target_Rows": "bigint",
        "Failed_Target_Rows": "bigint", "Start_Time": "timestamp",
        "End_Time": "timestamp",     "Error_Message": "varchar",
        "TotalTransErrors": "bigint","FirstErrorCode": "varchar",
    }

    def __init__(self, out_dir: str = "./dbt_output"):
        self.out = Path(out_dir)
        self.xp  = ExpressionTranspiler()

    # ------------------------------------------------------------------
    def generate(
        self,
        taskflows:    list[Taskflow],
        mappings:     list[Mapping],
        workflows:    list[Workflow],
        project_meta: Optional[dict] = None,
        connections:  Optional[list[dict]] = None,
        params:       Optional[dict] = None,
    ):
        for d in ("models/staging", "models/intermediate", "models/marts",
                  "seeds", "macros", "analyses"):
            (self.out / d).mkdir(parents=True, exist_ok=True)

        self._write_dbt_project(taskflows, mappings, workflows, project_meta)
        self._write_profiles(connections)
        self._write_macros()

        all_meta:    list[dict] = []
        all_sources: list[dict] = []

        for m in mappings:
            all_meta.extend(self._write_mapping_model(m))
            all_sources.extend(self._collect_mapping_sources(m))

        for tf in taskflows:
            all_meta.extend(self._write_tf_model(tf))
            all_sources.extend(self._collect_tf_sources(tf))

        for wf in workflows:
            all_meta.extend(self._write_workflow_model(wf, params or {}))

        self._write_schema_yml(all_meta)
        self._write_sources_yml(all_sources)
        self._write_seed_ddl(taskflows, mappings, workflows)
        self._write_lineage_report(mappings, taskflows, workflows, project_meta, connections, params)

        print(f"\n  Generated dbt project -> {self.out.resolve()}")
        print(f"   models/           - {len(all_meta)} SQL model(s)")
        print(f"   seeds/            - raw DDL")
        print(f"   macros/           - helpers")
        print(f"   lineage_report.md - lineage + params")

    # ------------------------------------------------------------------
    # dbt_project.yml
    # ------------------------------------------------------------------
    def _write_dbt_project(self, taskflows, mappings, workflows, project_meta):
        proj_name = "informatica_to_dbt"
        proj_desc = ""
        if project_meta:
            proj_name = _snake(project_meta.get("name", proj_name))
            proj_desc = f"# {project_meta.get('description','')}"
        _write(self.out / "dbt_project.yml", textwrap.dedent(f"""\
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
        """))

    # ------------------------------------------------------------------
    # profiles.yml
    # ------------------------------------------------------------------
    def _write_profiles(self, connections: Optional[list[dict]] = None):
        note = ""
        if connections:
            note = "  # connections: " + ", ".join(c["name"] for c in connections[:4])
        _write(self.out / "profiles.yml", textwrap.dedent(f"""\
            duckdb_local:
              target: dev
              outputs:
                dev:
                  type: duckdb
                  path: "dev.duckdb"
                  schema: main
                  threads: 4
                  {note}
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
        _write(self.out / "macros" / "run_ics_task.sql", textwrap.dedent("""\
            {% macro run_ics_task(task_name, guid, task_type='MCT') %}
                select
                    '{{ task_name }}'              as object_name,
                    {{ var('run_id',0) }}::bigint  as run_id,
                    {{ var('log_id',0) }}::bigint  as log_id,
                    '{{ guid }}'                   as task_id,
                    'SUCCEEDED'                    as task_status,
                    0::bigint as success_source_rows, 0::bigint as failed_source_rows,
                    0::bigint as success_target_rows, 0::bigint as failed_target_rows,
                    current_timestamp as start_time, current_timestamp as end_time,
                    null::varchar as error_message,
                    0::bigint as total_trans_errors, null::varchar as first_error_code
            {% endmacro %}
        """))
        _write(self.out / "macros" / "infa_lookup.sql", textwrap.dedent("""\
            {% macro infa_lookup(table, condition, cols) %}
                (select {{ cols | join(', ') }} from {{ table }} where {{ condition }} limit 1)
            {% endmacro %}
        """))
        _write(self.out / "macros" / "iif.sql", textwrap.dedent("""\
            {% macro iif(condition, true_val, false_val) %}
                CASE WHEN {{ condition }} THEN {{ true_val }} ELSE {{ false_val }} END
            {% endmacro %}
        """))

    # ------------------------------------------------------------------
    # Mapping -> dbt model
    # ------------------------------------------------------------------
    def _write_mapping_model(self, m: Mapping) -> list[dict]:
        prefix     = "mplt" if m.is_mapplet else "stg"
        model_name = f"{prefix}_{_snake(m.name)}"
        _write(self.out / "models" / "staging" / f"{model_name}.sql",
               self._mapping_to_sql(m, model_name))
        return [{"name": model_name,
                 "description": m.description or f"From Informatica {'mapplet' if m.is_mapplet else 'mapping'} '{m.name}' (folder: {m.folder}).",
                 "columns": self._output_columns(m), "layer": "staging"}]

    def _output_columns(self, m: Mapping) -> list[dict]:
        for inst_name in reversed(m.topo_sort()):
            inst = m.instances.get(inst_name)
            if not inst: continue
            t = m.transformations.get(inst.transformation_name)
            if t and t.ttype == "Output Transformation":
                return [{"name": f.name.lower(), "dtype": f.duckdb_type, "description": f.description} for f in t.inputs]
        for inst_name in reversed(m.topo_sort()):
            inst = m.instances.get(inst_name)
            if not inst: continue
            t = m.transformations.get(inst.transformation_name)
            if t and t.outputs:
                return [{"name": f.name.lower(), "dtype": f.duckdb_type, "description": f.description} for f in t.outputs]
        return []

    def _mapping_to_sql(self, m: Mapping, model_name: str) -> str:
        ordered  = m.topo_sort()
        upstream: dict[str, list[Connector]] = {n: [] for n in m.instances}
        for c in m.connectors:
            if c.to_instance in upstream:
                upstream[c.to_instance].append(c)

        header = textwrap.dedent(f"""\
            {{# Model: {model_name} | {'Mapplet' if m.is_mapplet else 'Mapping'}: {m.name} | Folder: {m.folder} #}}
            {{{{ config(materialized='view') }}}}
        """)

        ctes = [cte for n in ordered
                if (inst := m.instances.get(n)) and (t := m.transformations.get(inst.transformation_name))
                for cte in [self._instance_to_cte(n, inst, t, upstream, m)] if cte]

        if not ctes:
            return header + "\nselect 1 as placeholder\n"

        final = next(
            (n for n in reversed(ordered)
             if m.instances.get(n) and m.transformations.get(m.instances[n].transformation_name)
             and m.transformations[m.instances[n].transformation_name].ttype == "Output Transformation"),
            ordered[-1] if ordered else "unknown"
        )
        return header + "\nwith\n\n" + ",\n\n".join(ctes) + f"\n\nselect * from {_snake(final)}\n"

    def _instance_to_cte(self, inst_name, inst, t, upstream, m) -> str:
        n    = _snake(inst_name)
        ups  = upstream.get(inst_name, [])
        up_n = list(dict.fromkeys(_snake(c.from_instance) for c in ups))
        fc   = up_n[0] if up_n else "/* missing upstream */"

        if t.ttype in ("Input Transformation", "Source Qualifier", "Source Definition"):
            cols = ", ".join(f.name.lower() for f in t.outputs) or "*"
            src  = t.attributes.get("Table Name", t.attributes.get("Source table name", inst_name))
            return f"{n} as (\n    select {cols} from {{{{ source('raw', '{_snake(src)}') }}}}\n)"

        if t.ttype in ("Target Definition", "Output Transformation"):
            if not up_n: return ""
            col_map = {c.to_field: c.from_field for c in ups}
            cols = ", ".join(f"{col_map.get(f.name, f.name)} as {f.name.lower()}" for f in t.inputs)
            return f"{n} as (\n    select {cols}\n    from {up_n[0]}\n)"

        if t.ttype == "Expression":
            pass_map = {c.to_field: c.from_field for c in ups}
            parts  = [f"        {pass_map.get(f.name, f.name)} as {f.name.lower()}" for f in t.inputs]
            parts += [f"        {self.xp.transpile(f.expression)} as {f.name.lower()}  -- local" for f in t.locals]
            parts += [f"        {self.xp.transpile(f.expression) or f.name} as {f.name.lower()}" for f in t.outputs]
            return f"{n} as (\n    select\n        " + "\n        ,".join(parts) + f"\n    from {fc}\n)"

        if t.ttype == "Lookup Procedure":
            lkp    = t.lookup_table or "unknown_lookup"
            cond   = self.xp.transpile(t.lookup_condition)
            rets   = [f.name.lower() for f in t.fields if "RETURN" in f.port_type or ("OUTPUT" in f.port_type and "LOOKUP" in f.port_type)]
            ret_s  = ", ".join(f"lkp.{c}" for c in rets) or "lkp.*"
            lkp_s  = f"(\n        {t.lookup_sql.strip()}\n    )" if t.lookup_sql else f"{{{{ source('raw', '{_snake(lkp)}') }}}}"
            return (f"{n} as (\n    -- Lookup: {lkp}\n"
                    f"    select src.*, {ret_s}\n    from {fc} src\n"
                    f"    left join {lkp_s} lkp on {cond or '/* condition */'}\n)")

        if t.ttype == "Filter":
            cond = self.xp.transpile(t.filter_condition) or "TRUE"
            return f"{n} as (\n    select * from {fc} where {cond}\n)"

        if t.ttype == "Joiner":
            d2   = up_n[1] if len(up_n) > 1 else "detail_src"
            jc   = self.xp.transpile(t.attributes.get("Join Condition", "TRUE"))
            jt   = t.attributes.get("Join Type", "Normal").upper()
            sql_j = "INNER JOIN" if "NORMAL" in jt else "LEFT JOIN"
            return f"{n} as (\n    select m.*, d.* from {fc} m {sql_j} {d2} d on {jc}\n)"

        if t.ttype == "Aggregator":
            gc   = [f.name.lower() for f in t.inputs if not f.expression]
            ag   = [f"        {self.xp.transpile(f.expression)} as {f.name.lower()}" for f in t.outputs if f.expression]
            gs   = f"    group by {', '.join(gc)}" if gc else ""
            return (f"{n} as (\n    select\n        {', '.join(gc)}"
                    + (("\n        ," + "\n        ,".join(ag)) if ag else "")
                    + f"\n    from {fc}\n    {gs}\n)")

        if t.ttype == "Stored Procedure":
            ic = ", ".join(f.name.lower() for f in t.inputs)
            oc = ", ".join(f"null::{f.duckdb_type} as {f.name.lower()}" for f in t.outputs)
            return (f"{n} as (\n    -- SP: {t.sp_name}({ic}) TODO: implement as UDF\n"
                    f"    select src.*, {oc or 'null as proc_result'} from {fc} src\n)")

        if t.ttype == "Mapplet":
            ref = f"mplt_{_snake(inst.transformation_name)}"
            return f"{n} as (\n    select * from {{{{ ref('{ref}') }}}}\n    -- TODO: inputs from {fc}\n)"

        if t.ttype in ("Router", "Union Transformation"):
            return f"{n} as (\n    -- {t.ttype}: {inst_name} (pass-through)\n    select * from {fc}\n)"

        return f"{n} as (\n    -- {t.ttype}: {inst_name} (pass-through)\n    select * from {fc}\n)"

    # ------------------------------------------------------------------
    # Workflow -> dbt audit model
    # ------------------------------------------------------------------
    def _write_workflow_model(self, wf: Workflow, params: dict) -> list[dict]:
        model_name = f"int_{_snake(wf.name)}_wf_audit"
        unions = []
        for i, sess in enumerate(wf.sessions, 1):
            mapping_ref = f"stg_{_snake(sess.mapping_name)}" if sess.mapping_name else "/* no mapping */"
            src_conn = next(iter(sess.source_connections.values()), "")
            tgt_conn = next(iter(sess.target_connections.values()), "")
            # Resolve parameter substitution
            src_conn_resolved = self._resolve_param(src_conn, params, wf.name, sess.name)
            tgt_conn_resolved = self._resolve_param(tgt_conn, params, wf.name, sess.name)
            unions.append(
                f"    select {i}                           as step_order,\n"
                f"           '{sess.name}'                as session_name,\n"
                f"           '{sess.mapping_name}'        as mapping_name,\n"
                f"           '{src_conn_resolved}'        as source_connection,\n"
                f"           '{tgt_conn_resolved}'        as target_connection,\n"
                f"           'PENDING'                    as run_status,\n"
                f"           current_timestamp            as created_at\n"
                f"    -- ref: {{{{ ref('{mapping_ref}') }}}}"
            )
        union_sql = "\n    union all\n".join(unions) or "    select null as step_order"

        sql = textwrap.dedent(f"""\
            {{# Workflow: {wf.name} | Folder: {wf.folder} | Sessions: {len(wf.sessions)} #}}
            {{{{ config(materialized='table') }}}}

            with sessions as (
            {union_sql}
            )

            select
                step_order,
                session_name,
                mapping_name,
                source_connection,
                target_connection,
                run_status,
                created_at
            from sessions
            order by step_order
        """)
        _write(self.out / "models" / "intermediate" / f"{model_name}.sql", sql)
        return [{"name": model_name,
                 "description": wf.description or f"Audit model for PowerCenter workflow '{wf.name}'.",
                 "columns": [{"name": c, "dtype": "varchar", "description": ""}
                              for c in ["step_order","session_name","mapping_name",
                                        "source_connection","target_connection","run_status"]],
                 "layer": "intermediate"}]

    @staticmethod
    def _resolve_param(val: str, params: dict, wf_name: str, sess_name: str) -> str:
        """Replace $$PARAM or $DBConnection references using loaded param catalog."""
        if not val or not val.startswith(("$$", "$DB")):
            return val
        # Try most-specific section first, fall back to global
        for section in (f"{wf_name}.{sess_name}", wf_name, "__global__"):
            if section in params and val in params[section]:
                return params[section][val]
        return val

    # ------------------------------------------------------------------
    # Taskflow -> dbt models
    # ------------------------------------------------------------------
    def _write_tf_model(self, tf: Taskflow) -> list[dict]:
        meta: list[dict] = []
        for step in tf.steps:
            model_name = f"stg_{_snake(step.task_name)}"
            col_defs   = ",\n    ".join(f"{c}::{d}  as {c.lower()}" for c, d in self.ICS_OUTPUT_COLS.items())
            sql = textwrap.dedent(f"""\
                {{# Task: {step.task_name} | TF: {tf.name} #}}
                {{{{ config(materialized='view') }}}}
                with ics_raw as (
                    {{{{ run_ics_task(task_name='{step.task_name}', guid='{step.guid}', task_type='{step.task_type}') }}}}
                ),
                typed as (select {col_defs} from ics_raw)
                select * from typed
            """)
            _write(self.out / "models" / "staging" / f"{model_name}.sql", sql)
            meta.append({"name": model_name,
                          "description": f"Task '{step.task_name}' from taskflow '{tf.name}'.",
                          "columns": [{"name": c.lower(), "dtype": d, "description": ""}
                                      for c, d in self.ICS_OUTPUT_COLS.items()],
                          "layer": "staging"})

        orch = f"int_{_snake(tf.name)}_audit"
        unions = [f"    select {i} as step_order, '{s.task_name}' as task_name, * "
                  f"from {{{{ ref('stg_{_snake(s.task_name)}') }}}}"
                  for i, s in enumerate(tf.steps, 1)] or ["    select null, null"]
        _write(self.out / "models" / "intermediate" / f"{orch}.sql", textwrap.dedent(f"""\
            {{{{ config(materialized='table') }}}}
            with steps as ({"    union all".join(unions)})
            select step_order, task_name, task_status,
                   success_source_rows, failed_source_rows, success_target_rows, failed_target_rows,
                   total_trans_errors, first_error_code, error_message,
                   start_time, end_time,
                   datediff('second', start_time, end_time) as duration_seconds,
                   run_id, log_id, task_id, object_name
            from steps order by step_order
        """))
        meta.append({"name": orch, "description": tf.description or f"Audit for taskflow '{tf.name}'.",
                      "columns": [{"name": c, "dtype": "varchar", "description": ""} for c in
                                  ["step_order","task_name","task_status","duration_seconds"]],
                      "layer": "intermediate"})
        return meta

    # ------------------------------------------------------------------
    # schema.yml
    # ------------------------------------------------------------------
    def _write_schema_yml(self, meta: list[dict]):
        lines = ["version: 2", "", "models:"]
        for m in meta:
            lines += [f"  - name: {m['name']}", "    description: >",
                      f"      {m['description']}", "    columns:"]
            for col in m.get("columns", []):
                cname = col["name"] if isinstance(col, dict) else col.lower()
                lines.append(f"      - name: {cname}")
                if cname in ("run_id", "log_id"):
                    lines += ["        tests:", "          - not_null"]
                if cname == "task_status":
                    lines += ["        tests:", "          - accepted_values:",
                              "              values: ['SUCCEEDED','FAILED','RUNNING','STOPPED']"]
            lines.append("")
        _write(self.out / "models" / "schema.yml", "\n".join(lines))

    # ------------------------------------------------------------------
    # sources.yml
    # ------------------------------------------------------------------
    def _collect_mapping_sources(self, m: Mapping) -> list[dict]:
        srcs = []
        for inst_name, inst in m.instances.items():
            t = m.transformations.get(inst.transformation_name)
            if t and t.ttype in ("Input Transformation", "Source Qualifier", "Source Definition"):
                tbl = t.attributes.get("Table Name", t.attributes.get("Source table name", inst_name))
                srcs.append({"name": _snake(tbl), "description": f"Source for {m.name}/{inst_name}"})
        return srcs

    def _collect_tf_sources(self, tf: Taskflow) -> list[dict]:
        return [{"name": _snake(s.task_name), "description": f"Raw landing for {s.task_name}"} for s in tf.steps]

    def _write_sources_yml(self, sources: list[dict]):
        seen: set[str] = set()
        lines = ["version: 2", "", "sources:", "  - name: raw",
                 "    description: Raw landing schema", "    database: dev",
                 "    schema: raw", "    tables:"]
        for s in sources:
            if s["name"] in seen: continue
            seen.add(s["name"])
            lines += [f"      - name: {s['name']}", f"        description: \"{s['description']}\""]
        _write(self.out / "models" / "sources.yml", "\n".join(lines))

    # ------------------------------------------------------------------
    # Seed DDL
    # ------------------------------------------------------------------
    def _write_seed_ddl(self, taskflows, mappings, workflows):
        ics_cols = ",\n    ".join(f"{c.lower()}  {d}" for c, d in self.ICS_OUTPUT_COLS.items())
        blocks   = ["-- Auto-generated DuckDB DDL\n", "CREATE SCHEMA IF NOT EXISTS raw;\n"]
        for m in mappings:
            for inst_name, inst in m.instances.items():
                t = m.transformations.get(inst.transformation_name)
                if t and t.ttype in ("Input Transformation", "Source Qualifier", "Source Definition"):
                    tbl = t.attributes.get("Table Name", inst_name)
                    col_block = ",\n    ".join(f"{f.name.lower()}  {f.duckdb_type}" for f in t.outputs) or "id integer"
                    blocks.append(f"-- Mapping: {m.name}/{inst_name}\n"
                                  f"CREATE TABLE IF NOT EXISTS raw.{_snake(tbl)} (\n    {col_block},\n    _loaded_at timestamp default current_timestamp\n);\n")
        for tf in taskflows:
            for step in tf.steps:
                blocks.append(f"-- Task: {step.task_name}\n"
                              f"CREATE TABLE IF NOT EXISTS raw.{_snake(step.task_name)} (\n    {ics_cols},\n    _loaded_at timestamp default current_timestamp\n);\n")
        for wf in workflows:
            for sess in wf.sessions:
                if sess.mapping_name:
                    blocks.append(f"-- Session: {sess.name} (mapping: {sess.mapping_name})\n"
                                  f"-- raw table populated by mapping model stg_{_snake(sess.mapping_name)}\n")
        _write(self.out / "seeds" / "create_raw_tables.sql", "\n".join(blocks))

    # ------------------------------------------------------------------
    # Lineage report
    # ------------------------------------------------------------------
    def _write_lineage_report(self, mappings, taskflows, workflows, project_meta, connections, params):
        lines = ["# Informatica -> dbt Lineage Report\n"]
        if project_meta:
            lines += [f"**Project:** {project_meta['name']}",
                      f"**Description:** {project_meta.get('description','')}",
                      f"**Owner:** {project_meta.get('owner','')}", ""]
        if connections:
            lines += ["## Connections\n", "| Name | Type | Host | Database |",
                      "|------|------|------|----------|"]
            for c in connections:
                lines.append(f"| {c['name']} | {c.get('type','')} | {c.get('host','')} | {c.get('database','')} |")
            lines.append("")
        if params:
            lines += ["## Parameters\n", "| Section | Key | Value |", "|---------|-----|-------|"]
            for section, kvs in list(params.items())[:50]:
                for k, v in kvs.items():
                    lines.append(f"| {section} | {k} | {v} |")
            lines.append("")
        lines.append("## Mappings / Mapplets\n")
        for m in mappings:
            ordered   = m.topo_sort()
            dag_parts = []
            for inst_name in ordered:
                inst = m.instances.get(inst_name)
                t    = m.transformations.get(inst.transformation_name) if inst else None
                dag_parts.append(f"`{inst_name}` ({t.ttype if t else '?'})")
            lines += [f"### `{m.name}` ({'Mapplet' if m.is_mapplet else 'Mapping'}) -- `{m.folder}`\n",
                      "**DAG:** " + " -> ".join(dag_parts) + "\n",
                      "**Connectors:**\n"]
            for c in m.connectors:
                lines.append(f"  - `{c.from_instance}.{c.from_field}` -> `{c.to_instance}.{c.to_field}`")
            lines.append("\n**Expressions:**\n")
            for tname, t in m.transformations.items():
                for f in t.fields:
                    if f.expression:
                        raw = f.expression.replace("\r\n"," ").replace("\n"," ")[:80]
                        dbt = ExpressionTranspiler().transpile(f.expression)[:80]
                        lines += [f"  - `{tname}.{f.name}`",
                                  f"    - Infa   : `{raw}`",
                                  f"    - DuckDB : `{dbt}`"]
            lines.append("")
        lines.append("## Workflows\n")
        for wf in workflows:
            lines += [f"### `{wf.name}` -- `{wf.folder}`\n", "**Sessions:**\n"]
            for i, s in enumerate(wf.sessions, 1):
                lines.append(f"  {i}. `{s.name}` -> mapping:`{s.mapping_name}` "
                             f"src:`{next(iter(s.source_connections.values()), '')}` "
                             f"tgt:`{next(iter(s.target_connections.values()), '')}`")
            lines.append("")
        lines.append("## Taskflows\n")
        for tf in taskflows:
            lines += [f"### `{tf.name}`\n", f"{tf.description}\n", "**Steps:**\n"]
            for i, s in enumerate(tf.steps, 1):
                lines.append(f"  {i}. `{s.task_name}` ({s.task_type}) GUID:`{s.guid}`")
            lines.append("")
        _write(self.out / "lineage_report.md", "\n".join(lines))


# ===========================================================================
# CLI
# ===========================================================================
def main():
    ap = argparse.ArgumentParser(
        description="Convert Informatica (IDMC / PowerCenter) -> dbt/DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              # Local XML files
              python informatica_to_dbt.py --xml ./exports/ --json project.json --out ./out

              # IDMC live
              python informatica_to_dbt.py --idmc --token <tok> --pod-url https://usw1.dm-us.informaticacloud.com

              # PowerCenter SOAP
              python informatica_to_dbt.py --pc --pc-mode soap \\
                  --pc-host infa-host --pc-port 7333 --pc-repo DEV \\
                  --pc-user pmadmin --pc-password secret

              # PowerCenter direct DB (Oracle)
              python informatica_to_dbt.py --pc --pc-mode db \\
                  --pc-db-url "oracle+cx_oracle://user:pass@host:1521/SID"

              # PowerCenter XML exports + parameter file
              python informatica_to_dbt.py --pc --pc-mode xml --xml ./pc_exports/ \\
                  --pc-params ./params/DEV.par
        """),
    )

    mode_grp = ap.add_mutually_exclusive_group(required=True)
    mode_grp.add_argument("--xml",  metavar="PATH", help="Local XML file or directory")
    mode_grp.add_argument("--idmc", action="store_true", help="Fetch from IDMC REST API")
    mode_grp.add_argument("--pc",   action="store_true", help="Fetch from PowerCenter")

    # IDMC options
    ap.add_argument("--pod-url",    metavar="URL",   help="IDMC pod URL (or IDMC_POD_URL env)")
    ap.add_argument("--token",      metavar="TOKEN", help="IDMC Bearer token (or IDMC_TOKEN env)")
    ap.add_argument("--project-id", metavar="ID",    help="IDMC project filter")
    ap.add_argument("--fetch-only", action="store_true", help="IDMC: download raw files only")

    # PowerCenter options
    ap.add_argument("--pc-mode",     default="xml",
                    choices=["soap", "db", "xml"],
                    help="PowerCenter access mode: soap | db | xml (default: xml)")
    ap.add_argument("--pc-host",     metavar="HOST", help="PC repository host (SOAP)")
    ap.add_argument("--pc-port",     metavar="PORT", default="7333", help="PC SOAP port")
    ap.add_argument("--pc-repo",     metavar="NAME", help="PC repository name (SOAP)")
    ap.add_argument("--pc-user",     metavar="USER", help="PC username (SOAP)")
    ap.add_argument("--pc-password", metavar="PASS",
                    default=os.environ.get("PC_PASSWORD",""), help="PC password (or PC_PASSWORD env)")
    ap.add_argument("--pc-https",    action="store_true", help="Use HTTPS for SOAP (default: HTTP)")
    ap.add_argument("--pc-db-url",   metavar="URL",
                    help="SQLAlchemy DB URL for direct-DB mode, e.g. oracle+cx_oracle://u:p@h:1521/SID")
    ap.add_argument("--pc-params",   metavar="PATH", help="PC parameter file or directory (.par)")

    # Shared
    ap.add_argument("--json", metavar="PATH", help="IICS project JSON (local mode)")
    ap.add_argument("--out",  default="./dbt_output", help="Output directory")

    args = ap.parse_args()
    out  = Path(args.out)

    taskflows:    list[Taskflow] = []
    mappings:     list[Mapping]  = []
    workflows:    list[Workflow] = []
    connections:  list[dict]     = []
    params:       dict           = {}
    project_meta: Optional[dict] = None

    # ------------------------------------------------------------------
    # IDMC mode
    # ------------------------------------------------------------------
    if args.idmc:
        print("[IDMC] Connecting...")
        try:
            cfg     = IDMCConfig.from_args(args)
            client  = IDMCClient(cfg)
            fetcher = IDMCFetcher(client, out)
            print(f"  Pod : {cfg.pod_url}")
        except ValueError as e:
            print(f"Error: {e}"); sys.exit(1)

        mappings, taskflows, connections = fetcher.fetch_all(project_filter=args.project_id)

        if args.fetch_only:
            print(f"\n  Raw files -> {(out / '_idmc_raw').resolve()}")
            return

    # ------------------------------------------------------------------
    # PowerCenter mode
    # ------------------------------------------------------------------
    elif args.pc:
        print(f"[PowerCenter] mode={args.pc_mode}")
        pc_fetcher = PCFetcher(args, out)
        mappings, workflows, connections, params = pc_fetcher.fetch_all()

    # ------------------------------------------------------------------
    # Local XML mode
    # ------------------------------------------------------------------
    else:
        xml_files = collect_files(args.xml, (".xml",))
        print(f"Found {len(xml_files)} XML file(s).\n")
        tf_parser = InformaticaTaskflowParser()
        pm_parser = PowerMartParser()

        for xf in xml_files:
            print(f"  Parsing: {xf}")
            try:
                tf = tf_parser.parse_file(xf)
                print(f"    -> Taskflow '{tf.name}' ({len(tf.steps)} step(s))")
                taskflows.append(tf); continue
            except Exception:
                pass
            try:
                ms, wfs = pm_parser.parse_file(xf)
                for m in ms:
                    print(f"    -> {'Mapplet' if m.is_mapplet else 'Mapping'} '{m.name}' "
                          f"({len(m.transformations)} transforms, {len(m.connectors)} connectors)")
                for w in wfs:
                    print(f"    -> Workflow '{w.name}' ({len(w.sessions)} session(s))")
                mappings.extend(ms); workflows.extend(wfs)
            except Exception as e:
                print(f"    Warning: Skipped ({e})")

        if args.json:
            try:
                project_meta = ProjectJsonParser().parse_file(args.json)
                print(f"\n  Project: '{project_meta['name']}' -- {project_meta['description']}")
            except Exception as e:
                print(f"  Warning: project JSON skipped ({e})")

    # ------------------------------------------------------------------
    # Generate dbt project
    # ------------------------------------------------------------------
    if not taskflows and not mappings and not workflows:
        print("\nNothing parsed. Exiting."); sys.exit(1)

    gen = DbtGenerator(out_dir=str(out))
    gen.generate(taskflows, mappings, workflows, project_meta, connections, params)


if __name__ == "__main__":
    main()
