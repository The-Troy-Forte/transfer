"""
Informatica PowerCenter + IDMC -> dbt (DuckDB) Converter
=========================================================
Full mapping conversion covering all 12 transformation types,
source->target pair decomposition, SCD detection, expression
transpilation, lookup SQL inlining, and parameter substitution.

NEW: $$variable translation
  - $$PARAM values from .par files are substituted into expressions
    and SQL overrides at parse time.
  - $$DBConnection_* variables are resolved to dbt source() or
    profiles.yml connection names.
  - $$STORAGEFILEDIR / $$PMROOTDIR / $$INFA_HOME path variables
    are mapped to dbt seed-path or var() references.
  - Any remaining unresolved $$VAR is emitted as {{ var('VAR') }}
    so dbt's own variable system can resolve it at runtime.

Modes:
  --xml   Local PowerMart / Taskflow XML files
  --idmc  Live IDMC REST API (Bearer token)
  --pc    PowerCenter (SOAP | direct-DB | XML exports)

Install:
  pip install httpx python-dotenv dbt-duckdb
  pip install sqlalchemy cx_Oracle          # PC direct-DB Oracle
  pip install sqlalchemy pyodbc             # PC direct-DB SQL Server
  pip install requests lxml                 # PC SOAP
"""

import argparse, json, os, re, sys, time, textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

try:
    import httpx; _HTTP = "httpx"
except ImportError:
    import urllib.request; _HTTP = "urllib"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# $$Variable Translator  (module-level, no class dependency)
# ---------------------------------------------------------------------------
# Categories of PC $$variables and how they map to dbt constructs:
#
#   $$PARAM_*          -> literal value from .par catalog, or {{ var('PARAM_*') }}
#   $$DBConnection_*   -> dbt source schema name or profiles.yml key
#   $DBConnection_*    -> same (single-$ form used in session attributes)
#   $$STORAGEFILEDIR   -> {{ var('storage_file_dir', './data') }}
#   $$PMROOTDIR        -> {{ var('pm_root_dir', '.') }}
#   $$INFA_HOME        -> {{ var('infa_home', '.') }}
#   $$INFA_DOMAINS_FILE -> {{ var('infa_domains_file', '') }}
#   $$WorkflowName     -> {{ var('workflow_name', '') }}   (built-in PC variable)
#   $$FolderName       -> {{ var('folder_name', '') }}
#   $$MappingName      -> {{ var('mapping_name', '') }}
#   $$SessionName      -> {{ var('session_name', '') }}
#   $$StartTime        -> current_timestamp
#   $$SessStartTime    -> current_timestamp
#   $$PMFailureThreshold -> {{ var('pm_failure_threshold', '0') }}

# Built-in PC variables -> dbt/SQL equivalents
_BUILTIN_VAR_MAP: dict[str, str] = {
    "$$StartTime":           "current_timestamp",
    "$$SessStartTime":       "current_timestamp",
    "$$STORAGEFILEDIR":      "{{ var('storage_file_dir', './data') }}",
    "$$PMROOTDIR":           "{{ var('pm_root_dir', '.') }}",
    "$$INFA_HOME":           "{{ var('infa_home', '.') }}",
    "$$INFA_DOMAINS_FILE":   "{{ var('infa_domains_file', '') }}",
    "$$WorkflowName":        "{{ var('workflow_name', '') }}",
    "$$FolderName":          "{{ var('folder_name', '') }}",
    "$$MappingName":         "{{ var('mapping_name', '') }}",
    "$$SessionName":         "{{ var('session_name', '') }}",
    "$$PMFailureThreshold":  "{{ var('pm_failure_threshold', '0') }}",
    "$$PMSessionStatus":     "{{ var('pm_session_status', '') }}",
    "$$InputFileName":       "{{ var('input_file_name', '') }}",
    "$$OutputFileName":      "{{ var('output_file_name', '') }}",
    "$$BadFileName":         "{{ var('bad_file_name', '') }}",
}


def translate_variable(var_token: str, flat_params: dict[str, str]) -> str:
    """
    Translate a single $$VARIABLE or $DBConnection token to its dbt equivalent.

    Resolution order:
      1. Built-in PC system variable  -> hardcoded dbt/SQL mapping
      2. Parameter catalog match      -> literal value (quoted if string-like)
      3. DBConnection variable        -> {{ var('db_<name>') }} reference
      4. Unknown $$VAR                -> {{ var('<snake_name>') }}
    """
    # Normalise: ensure double-$ prefix for lookup
    normalised = var_token if var_token.startswith("$$") else f"${var_token}"

    # 1. Built-in system variable
    if normalised in _BUILTIN_VAR_MAP:
        return _BUILTIN_VAR_MAP[normalised]

    # 2. Parameter catalog match (try both $$KEY and $KEY forms)
    for key_form in (var_token, normalised, var_token.lstrip("$")):
        if key_form in flat_params:
            val = flat_params[key_form]
            # Quote string values that don't look like numbers or SQL keywords
            if val and not re.match(r"^[\d.\-]+$", val) and val.upper() not in ("TRUE","FALSE","NULL"):
                return f"'{val}'"
            return val

    # 3. DBConnection variable -> dbt var reference
    if "DBConnection" in normalised or "Connection" in normalised:
        conn_name = re.sub(r"\$+(DBConnection_?|Connection_?)", "", normalised, flags=re.IGNORECASE)
        return "{{{{ var('db_{conn}', '{conn}') }}}}".format(conn=_snake(conn_name))

    # 4. Generic fallback -> dbt var()
    var_name = _snake(normalised.lstrip("$"))
    return f"{{{{ var('{var_name}') }}}}"


def translate_variables(text: str, flat_params: dict[str, str]) -> tuple[str, list[str]]:
    """
    Scan text for all $$VARIABLE and $DBConnection_* tokens, translate each,
    and return (translated_text, list_of_unresolved_tokens).

    Tokens inside single-quoted SQL strings are also translated so that
    hardcoded connection names and paths in SQL overrides are replaced.
    """
    if not text:
        return text, []

    # Match $$WORD or $DBConnection_WORD (but not $$ alone)
    pattern = re.compile(r"\$\$[A-Za-z_]\w*|\$(?:DBConnection|Connection)_\w+", re.IGNORECASE)
    unresolved: list[str] = []
    result = []
    last = 0

    for m in pattern.finditer(text):
        token = m.group(0)
        replacement = translate_variable(token, flat_params)
        result.append(text[last:m.start()])
        result.append(replacement)
        last = m.end()
        # Track tokens that fell through to the generic var() fallback
        if replacement.startswith("{{ var(") and token not in _BUILTIN_VAR_MAP:
            unresolved.append(token)

    result.append(text[last:])
    return "".join(result), unresolved


def vars_to_dbt_project_vars(flat_params: dict[str, str]) -> dict[str, str]:
    """
    Convert all flat $$PARAM entries to dbt var() declarations
    suitable for inclusion in dbt_project.yml.
    Excludes built-in system variables (already handled by _BUILTIN_VAR_MAP).
    """
    builtins = set(_BUILTIN_VAR_MAP.keys())
    out: dict[str, str] = {}
    for key, val in flat_params.items():
        if key in builtins:
            continue
        dbt_key = _snake(key.lstrip("$"))
        out[dbt_key] = val
    return out


# ---------------------------------------------------------------------------
# IICS namespace / type maps
# ---------------------------------------------------------------------------
NS = {
    "sf":   "http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd",
    "repo": "http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd",
}

INFA_TYPE_MAP: dict[str, str] = {
    "string":"varchar","nstring":"varchar","char":"varchar","nchar":"varchar","text":"varchar",
    "double":"double","decimal":"decimal(38,10)","integer":"integer","smallinteger":"smallint",
    "bigint":"bigint","real":"float","float":"float",
    "date/time":"timestamp","date":"date","time":"time","binary":"blob",
}

# ---------------------------------------------------------------------------
# Database dialect support
# ---------------------------------------------------------------------------
DB_DIALECTS = ("duckdb", "snowflake", "azure_synapse", "mssql", "oracle")

_DIALECT_TYPE_MAP: dict[str, dict[str, str]] = {
    "duckdb": {
        "string":"varchar","nstring":"varchar","char":"varchar","nchar":"varchar","text":"varchar",
        "double":"double","decimal":"decimal(38,10)","integer":"integer","smallinteger":"smallint",
        "bigint":"bigint","real":"float","float":"float",
        "date/time":"timestamp","date":"date","time":"time","binary":"blob",
    },
    "snowflake": {
        "string":"varchar","nstring":"varchar","char":"char","nchar":"nchar","text":"text",
        "double":"float","decimal":"number(38,10)","integer":"integer","smallinteger":"smallint",
        "bigint":"bigint","real":"float","float":"float",
        "date/time":"timestamp_ntz","date":"date","time":"time","binary":"binary",
    },
    "azure_synapse": {
        "string":"nvarchar(max)","nstring":"nvarchar(max)","char":"char","nchar":"nchar","text":"nvarchar(max)",
        "double":"float","decimal":"decimal(38,10)","integer":"int","smallinteger":"smallint",
        "bigint":"bigint","real":"real","float":"float",
        "date/time":"datetime2","date":"date","time":"time","binary":"varbinary(max)",
    },
    "mssql": {
        "string":"nvarchar(4000)","nstring":"nvarchar(4000)","char":"char","nchar":"nchar","text":"nvarchar(max)",
        "double":"float","decimal":"decimal(38,10)","integer":"int","smallinteger":"smallint",
        "bigint":"bigint","real":"real","float":"float",
        "date/time":"datetime2","date":"date","time":"time","binary":"varbinary(max)",
    },
    "oracle": {
        "string":"varchar2(4000)","nstring":"nvarchar2(4000)","char":"char","nchar":"nchar","text":"clob",
        "double":"number","decimal":"number(38,10)","integer":"number(10)","smallinteger":"number(5)",
        "bigint":"number(19)","real":"binary_float","float":"binary_float",
        "date/time":"timestamp","date":"date","time":"varchar2(8)","binary":"blob",
    },
}

_DIALECT_PROFILES: dict[str, str] = {
    "duckdb": textwrap.dedent("""\
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
    """),
    "snowflake": textwrap.dedent("""\
        snowflake_profile:
          target: dev
          outputs:
            dev:
              type: snowflake
              account: "<account>.snowflakecomputing.com"
              user: "<user>"
              password: "<password>"
              role: "<role>"
              database: "<database>"
              warehouse: "<warehouse>"
              schema: "<schema>"
              threads: 4
              client_session_keep_alive: false
            prod:
              type: snowflake
              account: "<account>.snowflakecomputing.com"
              user: "<user>"
              password: "<password>"
              role: "<role>"
              database: "<database>"
              warehouse: "<warehouse>"
              schema: "<schema>"
              threads: 8
    """),
    "azure_synapse": textwrap.dedent("""\
        azure_synapse_profile:
          target: dev
          outputs:
            dev:
              type: synapse
              driver: "ODBC Driver 18 for SQL Server"
              host: "<workspace>.sql.azuresynapse.net"
              port: 1433
              database: "<database>"
              schema: "<schema>"
              authentication: ActiveDirectoryInteractive
              client_id: "<client_id>"
              client_secret: "<client_secret>"
              tenant_id: "<tenant_id>"
              threads: 4
            prod:
              type: synapse
              driver: "ODBC Driver 18 for SQL Server"
              host: "<workspace>.sql.azuresynapse.net"
              port: 1433
              database: "<database>"
              schema: "<schema>"
              authentication: ActiveDirectoryInteractive
              threads: 8
    """),
    "mssql": textwrap.dedent("""\
        mssql_profile:
          target: dev
          outputs:
            dev:
              type: sqlserver
              driver: "ODBC Driver 18 for SQL Server"
              server: "<server>.database.windows.net"
              port: 1433
              database: "<database>"
              schema: "<schema>"
              authentication: sql
              username: "<user>"
              password: "<password>"
              threads: 4
            prod:
              type: sqlserver
              driver: "ODBC Driver 18 for SQL Server"
              server: "<server>.database.windows.net"
              port: 1433
              database: "<database>"
              schema: "<schema>"
              authentication: sql
              username: "<user>"
              password: "<password>"
              threads: 8
    """),
    "oracle": textwrap.dedent("""\
        oracle_profile:
          target: dev
          outputs:
            dev:
              type: oracle
              user: "<user>"
              password: "<password>"
              host: "<host>"
              port: 1521
              database: "<service_name>"
              schema: "<schema>"
              threads: 4
            prod:
              type: oracle
              user: "<user>"
              password: "<password>"
              host: "<host>"
              port: 1521
              database: "<service_name>"
              schema: "<schema>"
              threads: 8
    """),
}

_DIALECT_PACKAGES: dict[str, str] = {
    "duckdb":        "dbt-duckdb",
    "snowflake":     "dbt-snowflake",
    "azure_synapse": "dbt-synapse",
    "mssql":         "dbt-sqlserver",
    "oracle":        "dbt-oracle",
}

_DIALECT_SQL: dict[str, dict[str, str]] = {
    "duckdb":        {"current_ts": "current_timestamp", "row_num": "row_number() over ()",
                      "datediff": "datediff('second', {0}, {1})", "quote": '"'},
    "snowflake":     {"current_ts": "current_timestamp()", "row_num": "row_number() over (order by 1)",
                      "datediff": "datediff(second, {0}, {1})", "quote": '"'},
    "azure_synapse": {"current_ts": "getdate()", "row_num": "row_number() over (order by (select null))",
                      "datediff": "datediff(second, {0}, {1})", "quote": "["},
    "mssql":         {"current_ts": "getdate()", "row_num": "row_number() over (order by (select null))",
                      "datediff": "datediff(second, {0}, {1})", "quote": "["},
    "oracle":        {"current_ts": "systimestamp", "row_num": "rownum",
                      "datediff": "extract(second from ({1} - {0}))", "quote": '"'},
}

def dialect_type(infa_type: str, dialect: str) -> str:
    tmap = _DIALECT_TYPE_MAP.get(dialect, _DIALECT_TYPE_MAP["duckdb"])
    return tmap.get(infa_type.lower(), "varchar")

# ---------------------------------------------------------------------------
# Expression Transpiler  — full Informatica -> DuckDB SQL
# ---------------------------------------------------------------------------
class ExpressionTranspiler:
    """
    Converts Informatica expressions to DuckDB-compatible SQL.
    $$variable tokens are resolved via translate_variables() before
    any function rewriting so that substituted values participate
    correctly in IIF/DECODE/etc. rewrites.
    """

    _RENAMES: dict[str, str] = {
        "ISNULL":       "({0} IS NULL)",
        "IS_SPACES":    "(TRIM({0}) = '')",
        "NVL":          "COALESCE({0}, {1})",
        "NVL2":         "CASE WHEN {0} IS NOT NULL THEN {1} ELSE {2} END",
        "UPPER":        "UPPER({0})",
        "LOWER":        "LOWER({0})",
        "LTRIM":        "LTRIM({0})",
        "RTRIM":        "RTRIM({0})",
        "TRIM":         "TRIM({0})",
        "LENGTH":       "LENGTH({0})",
        "INSTR":        "INSTR({0}, {1})",
        "SUBSTR":       "SUBSTRING({0}, {1}, {2})",
        "LPAD":         "LPAD({0}, {1}, {2})",
        "RPAD":         "RPAD({0}, {1}, {2})",
        "REPLACE":      "REPLACE({0}, {1}, {2})",
        "CONCAT":       "CONCAT({0}, {1})",
        "TO_CHAR":      "CAST({0} AS VARCHAR)",
        "TO_INTEGER":   "CAST({0} AS INTEGER)",
        "TO_DECIMAL":   "CAST({0} AS DECIMAL(38,10))",
        "TO_FLOAT":     "CAST({0} AS FLOAT)",
        "ABS":          "ABS({0})",
        "CEIL":         "CEIL({0})",
        "FLOOR":        "FLOOR({0})",
        "MOD":          "({0} % {1})",
        "ROUND":        "ROUND({0}, {1})",
        "TRUNC":        "TRUNC({0})",
        "SQRT":         "SQRT({0})",
        "POWER":        "POWER({0}, {1})",
        "EXP":          "EXP({0})",
        "LOG":          "LN({0})",
        "ADD_TO_DATE":  "({0} + INTERVAL {1} {2})",
        "LAST_DAY":     "LAST_DAY({0})",
        "DATE_DIFF":    "DATEDIFF('{2}', {0}, {1})",
        "GET_DATE_PART":"DATE_PART('{1}', {0})",
        "SYSDATE":      "CURRENT_TIMESTAMP",
        "SYSTIMESTAMP": "CURRENT_TIMESTAMP",
        "SUM":          "SUM({0})",
        "AVG":          "AVG({0})",
        "MIN":          "MIN({0})",
        "MAX":          "MAX({0})",
        "COUNT":        "COUNT({0})",
        "FIRST":        "FIRST({0})",
        "LAST":         "LAST({0})",
        "MEDIAN":       "MEDIAN({0})",
        "STDDEV":       "STDDEV({0})",
        "VARIANCE":     "VARIANCE({0})",
        "ERROR":        "NULL  /* ERROR({0}) */",
        "ABORT":        "NULL  /* ABORT({0}) */",
    }

    def __init__(self, params: Optional[dict] = None):
        self._params = params or {}

    def transpile(self, expr: str) -> str:
        if not expr:
            return ""
        out = expr
        # XML entities
        out = (out.replace("&apos;", "'").replace("&amp;", "&")
                  .replace("&#xD;&#xA;", "\n").replace("&#xA;", "\n"))
        # Strip SP prefix
        out = re.sub(r":SP\.", "", out)
        # $$variable substitution FIRST — values must be resolved before
        # function rewrites so IIF($$PARAM = 'X', ...) works correctly
        out, _ = translate_variables(out, self._params)
        # Multi-step rewrites
        out = self._iif_to_case(out)
        out = self._decode_to_case(out)
        out = self._to_date(out)
        out = self._apply_renames(out)
        return out.strip()

    def _apply_renames(self, expr: str) -> str:
        for infa_fn, tmpl in self._RENAMES.items():
            pattern = re.compile(rf"\b{infa_fn}\s*\(", re.IGNORECASE)
            result, i = [], 0
            while i < len(expr):
                m = pattern.search(expr, i)
                if not m:
                    result.append(expr[i:]); break
                result.append(expr[i:m.start()])
                args_str, end = self._extract_args_str(expr, m.end())
                args = self._split_args(args_str)
                filled = tmpl
                for idx, arg in enumerate(args):
                    filled = filled.replace(f"{{{idx}}}", arg.strip())
                result.append(filled)
                i = end
            expr = "".join(result)
        return expr

    @staticmethod
    def _iif_to_case(expr: str) -> str:
        pattern = re.compile(r"\bIIF\s*\(", re.IGNORECASE)
        result, i = [], 0
        while i < len(expr):
            m = pattern.search(expr, i)
            if not m:
                result.append(expr[i:]); break
            result.append(expr[i:m.start()])
            args_str, end = ExpressionTranspiler._extract_args_str(expr, m.end())
            args = ExpressionTranspiler._split_args(args_str)
            if len(args) >= 3:
                result.append(
                    f"CASE WHEN {args[0].strip()} THEN {args[1].strip()} "
                    f"ELSE {args[2].strip()} END"
                )
            else:
                result.append(f"IIF({args_str})")
            i = end
        return "".join(result)

    @staticmethod
    def _decode_to_case(expr: str) -> str:
        pattern = re.compile(r"\bDECODE\s*\(", re.IGNORECASE)
        result, i = [], 0
        while i < len(expr):
            m = pattern.search(expr, i)
            if not m:
                result.append(expr[i:]); break
            result.append(expr[i:m.start()])
            args_str, end = ExpressionTranspiler._extract_args_str(expr, m.end())
            args = [a.strip() for a in ExpressionTranspiler._split_args(args_str)]
            if len(args) >= 3:
                col = args[0]; pairs = args[1:]
                whens = []; j = 0
                while j + 1 < len(pairs):
                    whens.append(f"WHEN {col} = {pairs[j]} THEN {pairs[j+1]}")
                    j += 2
                default = f"ELSE {pairs[j]}" if j < len(pairs) else ""
                result.append(f"CASE {' '.join(whens)} {default} END")
            else:
                result.append(f"DECODE({args_str})")
            i = end
        return "".join(result)

    @staticmethod
    def _to_date(expr: str) -> str:
        pattern = re.compile(r"\bTO_DATE\s*\(", re.IGNORECASE)
        result, i = [], 0
        while i < len(expr):
            m = pattern.search(expr, i)
            if not m:
                result.append(expr[i:]); break
            result.append(expr[i:m.start()])
            args_str, end = ExpressionTranspiler._extract_args_str(expr, m.end())
            args = ExpressionTranspiler._split_args(args_str)
            if len(args) == 2:
                fmt = args[1].strip().strip("'")
                fmt = (fmt.replace("YYYY", "%Y").replace("MM", "%m")
                           .replace("DD", "%d").replace("HH24", "%H")
                           .replace("HH", "%I").replace("MI", "%M")
                           .replace("SS", "%S"))
                result.append(f"STRPTIME({args[0].strip()}, '{fmt}')")
            else:
                result.append(f"TO_DATE({args_str})")
            i = end
        return "".join(result)

    @staticmethod
    def _extract_args_str(expr: str, start: int) -> tuple[str, int]:
        depth, j, buf = 1, start, []
        while j < len(expr) and depth > 0:
            ch = expr[j]
            if ch == "(": depth += 1
            elif ch == ")": depth -= 1
            if depth > 0: buf.append(ch)
            j += 1
        return "".join(buf), j

    @staticmethod
    def _split_args(s: str) -> list[str]:
        args, depth, buf, in_str = [], 0, [], False
        prev = ""
        for ch in s:
            if ch == "'" and prev != "\\":
                in_str = not in_str
            if not in_str:
                if ch == "(": depth += 1
                elif ch == ")": depth -= 1
            if ch == "," and depth == 0 and not in_str:
                args.append("".join(buf)); buf = []
            else:
                buf.append(ch)
            prev = ch
        if buf:
            args.append("".join(buf))
        return args


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
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
    group_name: str = ""

    @property
    def duckdb_type(self) -> str:
        return INFA_TYPE_MAP.get(self.datatype.lower(), "varchar")

    def native_type(self, dialect: str = "duckdb") -> str:
        return dialect_type(self.datatype, dialect)

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
    groups: dict[str, list[TransformField]] = field(default_factory=dict)
    description: str = ""
    reusable: bool = False

    @property
    def inputs(self): return [f for f in self.fields if f.is_input]
    @property
    def outputs(self): return [f for f in self.fields if f.is_output]
    @property
    def locals(self): return [f for f in self.fields if f.is_local]
    @property
    def lookup_table(self): return self.attributes.get("Lookup table name", "")
    @property
    def lookup_sql(self): return self.attributes.get("Lookup Sql Override", "")
    @property
    def lookup_condition(self): return self.attributes.get("Lookup condition", "")
    @property
    def filter_condition(self): return self.attributes.get("Filter Condition", "")
    @property
    def update_strategy(self): return self.attributes.get("Update Strategy Expression", "DD_INSERT")
    @property
    def sp_name(self): return self.attributes.get("Stored Procedure Name", self.name)
    @property
    def seq_start(self): return int(self.attributes.get("Start Value", "1"))
    @property
    def seq_increment(self): return int(self.attributes.get("Increment By", "1"))
    @property
    def rank_by(self): return self.attributes.get("Rank By", "")
    @property
    def rank_count(self): return self.attributes.get("Top/Bottom", "10")
    @property
    def normalizer_occur(self): return int(self.attributes.get("Number Of Occurrences", "1"))


@dataclass
class MappingInstance:
    name: str
    transformation_name: str
    transformation_type: str
    reusable: bool = False


@dataclass
class SourceTargetPair:
    mapping_name: str
    folder: str
    source_instance: str
    source_table: str
    target_instance: str
    target_table: str
    transformations: list[str]
    is_scd2: bool = False
    scd_key_cols: list[str] = field(default_factory=list)
    scd_hist_cols: list[str] = field(default_factory=list)
    update_strategy: str = "DD_INSERT"


@dataclass
class Mapping:
    name: str
    folder: str
    description: str = ""
    transformations: dict[str, Transformation] = field(default_factory=dict)
    instances: dict[str, MappingInstance] = field(default_factory=dict)
    connectors: list[Connector] = field(default_factory=list)
    is_mapplet: bool = False
    source_target_pairs: list[SourceTargetPair] = field(default_factory=list)

    def topo_sort(self) -> list[str]:
        deps: dict[str, set] = {n: set() for n in self.instances}
        for c in self.connectors:
            fi = c.from_instance; ti = c.to_instance
            if fi not in deps: deps[fi] = set()
            if ti not in deps: deps[ti] = set()
            deps[ti].add(fi)
        ordered, visited = [], set()
        def visit(n):
            if n in visited: return
            visited.add(n)
            for d in deps.get(n, []): visit(d)
            ordered.append(n)
        for n in list(deps): visit(n)
        return ordered

    def _ttype(self, inst_name: str) -> str:
        inst = self.instances.get(inst_name)
        if inst is None: return ""
        t = self.transformations.get(inst.transformation_name)
        return t.ttype if t is not None else inst.transformation_type or ""

    def detect_source_target_pairs(self):
        sources = [n for n in self.instances if self._ttype(n) in
                   ("Input Transformation","Source Qualifier","Source Definition")]
        targets = [n for n in self.instances if self._ttype(n) in
                   ("Output Transformation","Target Definition")]
        fwd: dict[str, set[str]] = {n: set() for n in self.instances}
        for c in self.connectors:
            fwd.setdefault(c.from_instance, set()).add(c.to_instance)
        for src in sources:
            visited_path: dict[str, list[str]] = {src: [src]}
            queue = [src]
            while queue:
                cur = queue.pop(0)
                for nxt in fwd.get(cur, []):
                    if nxt not in visited_path:
                        visited_path[nxt] = visited_path[cur] + [nxt]
                        queue.append(nxt)
            for tgt in targets:
                if tgt in visited_path:
                    path = visited_path[tgt]
                    src_inst = self.instances.get(src)
                    tgt_inst = self.instances.get(tgt)
                    src_t = self.transformations.get(src_inst.transformation_name) if src_inst else None
                    tgt_t = self.transformations.get(tgt_inst.transformation_name) if tgt_inst else None
                    src_table = (src_t.attributes.get("Table Name",
                                 src_t.attributes.get("Source table name", src)) if src_t else src)
                    tgt_table = (tgt_t.attributes.get("Table Name",
                                 tgt_t.attributes.get("Target table name", tgt)) if tgt_t else tgt)
                    upd_strat, is_scd2 = "DD_INSERT", False
                    key_cols, hist_cols = [], []
                    for inst_name in path:
                        inst = self.instances.get(inst_name)
                        t = self.transformations.get(inst.transformation_name) if inst else None
                        if t is None: continue
                        if t.ttype == "Update Strategy":
                            upd_strat = t.update_strategy
                            if "DD_UPDATE" in upd_strat.upper() or "2" in upd_strat:
                                is_scd2 = True
                        if t.ttype == "Expression":
                            for f in t.fields:
                                n_low = f.name.lower()
                                if any(k in n_low for k in ("_key","_id","key_col")) and f.is_output:
                                    key_cols.append(f.name)
                                if any(k in n_low for k in ("eff_","expir","start_dt","end_dt","curr_","hist_")) and f.is_output:
                                    hist_cols.append(f.name)
                    self.source_target_pairs.append(SourceTargetPair(
                        mapping_name=self.name, folder=self.folder,
                        source_instance=src, source_table=src_table,
                        target_instance=tgt, target_table=tgt_table,
                        transformations=path, is_scd2=is_scd2,
                        scd_key_cols=key_cols, scd_hist_cols=hist_cols,
                        update_strategy=upd_strat,
                    ))


@dataclass
class SessionTask:
    name: str
    mapping_name: str
    folder: str
    source_connections: dict[str, str] = field(default_factory=dict)
    target_connections: dict[str, str] = field(default_factory=dict)
    parameters: dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class Workflow:
    name: str
    folder: str
    description: str = ""
    sessions: list[SessionTask] = field(default_factory=list)
    parameters: dict[str, str] = field(default_factory=dict)


@dataclass
class MappingStep:
    step_id: str; title: str; task_name: str; guid: str; task_type: str
    wait: bool = True; max_wait: int = 604800
    outputs: dict = field(default_factory=dict)
    error_handler: str = "suspend"; next_step: Optional[str] = None


@dataclass
class Taskflow:
    name: str; description: str
    steps: list[MappingStep] = field(default_factory=list)
    step_order: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# PowerMart XML Parser
# ---------------------------------------------------------------------------
class PowerMartParser:

    def parse_file(self, xml_path: str) -> tuple[list[Mapping], list[Workflow]]:
        try:
            tree = ET.parse(xml_path)
        except ET.ParseError:
            with open(xml_path, encoding="latin-1") as fh:
                tree = ET.parse(fh)
        root = tree.getroot()
        mappings:  list[Mapping]  = []
        workflows: list[Workflow] = []
        for repo in root.iter("REPOSITORY"):
            for folder in repo.iter("FOLDER"):
                fn = folder.get("NAME", "")
                for el in folder.iter("MAPPLET"):
                    m = self._parse_mapping_el(el, fn, True)
                    if m: mappings.append(m)
                for el in folder.findall("MAPPING"):
                    m = self._parse_mapping_el(el, fn, False)
                    if m: mappings.append(m)
                for el in folder.findall("WORKFLOW"):
                    w = self._parse_workflow(el, fn)
                    if w: workflows.append(w)
        for m in mappings:
            m.detect_source_target_pairs()
        return mappings, workflows

    def _parse_mapping_el(self, el: ET.Element, folder: str, is_mapplet: bool) -> Optional[Mapping]:
        m = Mapping(name=el.get("NAME",""), folder=folder,
                    description=el.get("DESCRIPTION",""), is_mapplet=is_mapplet)
        for t in el.findall("TRANSFORMATION"):
            trans = self._parse_transformation(t)
            m.transformations[trans.name] = trans
        for inst in el.findall("INSTANCE"):
            i = MappingInstance(
                name=inst.get("NAME",""),
                transformation_name=inst.get("TRANSFORMATION_NAME", inst.get("NAME","")),
                transformation_type=inst.get("TRANSFORMATION_TYPE",""),
                reusable=inst.get("REUSABLE","NO").upper()=="YES",
            )
            m.instances[i.name] = i
        if not m.instances:
            for tn, tr in m.transformations.items():
                m.instances[tn] = MappingInstance(tn, tn, tr.ttype)
        for c_el in el.findall("CONNECTOR"):
            from_inst  = c_el.get("FROMINSTANCE", "").strip()
            from_field = c_el.get("FROMFIELD",    "").strip()
            to_inst    = c_el.get("TOINSTANCE",   "").strip()
            to_field   = c_el.get("TOFIELD",      "").strip()
            for inst_name, trans_name, trans_type in (
                (from_inst, c_el.get("FROMINSTANCETYPE", from_inst), c_el.get("FROMINSTANCETYPE","")),
                (to_inst,   c_el.get("TOINSTANCETYPE",   to_inst),   c_el.get("TOINSTANCETYPE",  "")),
            ):
                if inst_name and inst_name not in m.instances:
                    m.instances[inst_name] = MappingInstance(
                        name=inst_name,
                        transformation_name=trans_name or inst_name,
                        transformation_type=trans_type or "Unknown",
                        reusable=True,
                    )
            if from_inst and to_inst:
                m.connectors.append(Connector(
                    from_instance=from_inst, from_field=from_field,
                    to_instance=to_inst,     to_field=to_field,
                ))
            else:
                print(f"    Warning: skipping incomplete CONNECTOR "
                      f"FROMINSTANCE='{from_inst}' TOINSTANCE='{to_inst}' "
                      f"in mapping '{m.name}'")
        return m if (m.transformations or m.connectors) else None

    def _parse_transformation(self, el: ET.Element) -> Transformation:
        t = Transformation(name=el.get("NAME",""), ttype=el.get("TYPE",""),
                           description=el.get("DESCRIPTION",""),
                           reusable=el.get("REUSABLE","NO").upper()=="YES")
        for tf in el.findall("TRANSFORMFIELD"):
            grp = tf.get("GROUP","")
            f = TransformField(
                name=tf.get("NAME",""), datatype=tf.get("DATATYPE","string"),
                port_type=tf.get("PORTTYPE","INPUT"), expression=tf.get("EXPRESSION",""),
                precision=int(tf.get("PRECISION",4000)), scale=int(tf.get("SCALE",0)),
                default_value=tf.get("DEFAULTVALUE",""), description=tf.get("DESCRIPTION",""),
                group_name=grp,
            )
            t.fields.append(f)
            if grp:
                t.groups.setdefault(grp, []).append(f)
        for gf in el.findall("GROUPFILTER"):
            grp = gf.get("NAME",""); cond = gf.get("CONDITION","")
            t.attributes[f"__group_filter_{grp}"] = cond
        for ta in el.findall("TABLEATTRIBUTE"):
            t.attributes[ta.get("NAME","")] = ta.get("VALUE","")
        return t

    def _parse_workflow(self, el: ET.Element, folder: str) -> Optional[Workflow]:
        wf = Workflow(name=el.get("NAME",""), folder=folder, description=el.get("DESCRIPTION",""))
        for task in el.iter("TASKINSTANCE"):
            if task.get("TASKTYPE","") == "SESSION":
                s = self._parse_session(task, folder)
                if s: wf.sessions.append(s)
        for sess_el in el.findall("SESSION"):
            s = self._parse_session_el(sess_el, folder)
            if s: wf.sessions.append(s)
        return wf if wf.sessions else None

    def _parse_session(self, el: ET.Element, folder: str) -> Optional[SessionTask]:
        name = el.get("NAME", el.get("TASKNAME",""))
        mapping, src_c, tgt_c, params = "", {}, {}, {}
        for attr in el.findall("ATTRIBUTE"):
            an, av = attr.get("NAME",""), attr.get("VALUE","")
            if an == "Mapping name": mapping = av
            elif "source" in an.lower() and "connection" in an.lower(): src_c[an] = av
            elif "target" in an.lower() and "connection" in an.lower(): tgt_c[an] = av
            else: params[an] = av
        return SessionTask(name=name, mapping_name=mapping, folder=folder,
                           source_connections=src_c, target_connections=tgt_c, parameters=params)

    def _parse_session_el(self, el: ET.Element, folder: str) -> Optional[SessionTask]:
        name = el.get("NAME",""); mapping = el.get("MAPPINGNAME","")
        src_c, tgt_c, params = {}, {}, {}
        for attr in el.findall(".//ATTRIBUTE"):
            an, av = attr.get("NAME",""), attr.get("VALUE","")
            if "source" in an.lower() and "connection" in an.lower(): src_c[an] = av
            elif "target" in an.lower() and "connection" in an.lower(): tgt_c[an] = av
            else: params[an] = av
        return SessionTask(name=name, mapping_name=mapping, folder=folder,
                           source_connections=src_c, target_connections=tgt_c, parameters=params)


# ---------------------------------------------------------------------------
# Parameter file parser
# ---------------------------------------------------------------------------
class ParameterFileParser:
    def parse_file(self, path: str) -> dict[str, dict[str, str]]:
        catalog: dict[str, dict[str, str]] = {}
        current: dict[str, str] = {}
        section = "__global__"
        with open(path, encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"): continue
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
        all_p: dict[str, dict[str, str]] = {}
        for f in collect_files(path, (".par",".properties",".param")):
            all_p.update(self.parse_file(f))
        return all_p

    def flat_params(self, catalog: dict) -> dict[str, str]:
        flat: dict[str, str] = {}
        for section_vals in catalog.values():
            for k, v in section_vals.items():
                flat[k] = v
        return flat


# ---------------------------------------------------------------------------
# IICS Taskflow Parser
# ---------------------------------------------------------------------------
class InformaticaTaskflowParser:

    def parse_file(self, xml_path: str) -> Taskflow:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        entry   = root.find(".//{%s}Entry" % NS["repo"])
        tf_root = entry.find("{%s}taskflow" % NS["sf"]) if entry is not None else None
        if tf_root is None:
            tf_root = root.find(".//{%s}taskflow" % NS["sf"])
        if tf_root is None:
            raise ValueError(f"No <taskflow> in {xml_path}")
        name    = tf_root.get("name", Path(xml_path).stem)
        desc_el = tf_root.find("{%s}description" % NS["sf"])
        description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
        tf = Taskflow(name=name, description=description)
        self._parse_flow(tf_root, tf)
        return tf

    def _parse_flow(self, tf_root, tf):
        flow = tf_root.find("{%s}flow" % NS["sf"])
        if flow is None: return
        containers = {ec.get("id"): ec for ec in flow.findall("{%s}eventContainer" % NS["sf"])}
        start = flow.find("{%s}start" % NS["sf"])
        ids = []
        if start is not None:
            cur = self._link(start)
            while cur and cur in containers:
                ids.append(cur); cur = self._link(containers[cur])
        tf.step_order = ids
        for eid in ids:
            s = self._parse_ec(eid, containers[eid])
            if s: tf.steps.append(s)

    def _link(self, el) -> Optional[str]:
        lnk = el.find("{%s}link" % NS["sf"])
        return lnk.get("targetId") if lnk is not None else None

    def _parse_ec(self, ec_id, ec) -> Optional[MappingStep]:
        svc = ec.find("{%s}service" % NS["sf"])
        if svc is None: return None
        title = (svc.findtext("{%s}title" % NS["sf"]) or "").strip()
        params = {}
        si = svc.find("{%s}serviceInput" % NS["sf"])
        if si is not None:
            for p in si.findall("{%s}parameter" % NS["sf"]):
                params[p.get("name","")] = p.text or ""
        outputs = {}
        so = svc.find("{%s}serviceOutput" % NS["sf"])
        if so is not None:
            for op in so.findall("{%s}operation" % NS["sf"]):
                col = re.sub(r"^temp\.[^/]+/output/","", op.get("to",""))
                outputs[col] = op.text or ""
        error_handler = "suspend"
        events = ec.find("{%s}events" % NS["sf"])
        if events is not None:
            for catch in events.findall("{%s}catch" % NS["sf"]):
                if catch.get("name") == "error":
                    error_handler = "suspend" if catch.find("{%s}suspend" % NS["sf"]) is not None else "continue"
        return MappingStep(
            step_id=ec_id, title=title or params.get("Task Name",""),
            task_name=params.get("Task Name",""), guid=params.get("GUID",""),
            task_type=params.get("Task Type","MCT"),
            wait=params.get("Wait for Task to Complete","true").lower()=="true",
            max_wait=int(params.get("Max Wait","604800")),
            outputs=outputs, error_handler=error_handler, next_step=self._link(ec),
        )


# ---------------------------------------------------------------------------
# Project JSON parser
# ---------------------------------------------------------------------------
class ProjectJsonParser:
    def parse_file(self, json_path: str) -> dict:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        props = {p["name"]: p["value"] for p in data.get("properties",[]) if "name" in p}
        return {"id": props.get("id",""), "name": props.get("name",""),
                "description": props.get("description",""),
                "owner": props.get("owner",""), "created": props.get("createdTime",""),
                "modified": props.get("lastUpdatedTime",""), "state": props.get("documentState","")}


# ---------------------------------------------------------------------------
# dbt / DuckDB Generator
# ---------------------------------------------------------------------------
class DbtGenerator:

    ICS_COLS: dict[str, str] = {
        "Object_Name":"varchar","Run_Id":"bigint","Log_Id":"bigint","Task_Id":"varchar",
        "Task_Status":"varchar","Success_Source_Rows":"bigint","Failed_Source_Rows":"bigint",
        "Success_Target_Rows":"bigint","Failed_Target_Rows":"bigint",
        "Start_Time":"timestamp","End_Time":"timestamp","Error_Message":"varchar",
        "TotalTransErrors":"bigint","FirstErrorCode":"varchar",
    }

    def __init__(self, out_dir: str = "./dbt_output",
                 flat_params: Optional[dict] = None,
                 src_dialect: str = "duckdb",
                 tgt_dialect: str = "duckdb"):
        self.out         = Path(out_dir)
        self.flat_params = flat_params or {}
        self.xp          = ExpressionTranspiler(self.flat_params)
        self.src_dialect = src_dialect
        self.tgt_dialect = tgt_dialect
        self._dsql       = _DIALECT_SQL.get(tgt_dialect, _DIALECT_SQL["duckdb"])

    # ------------------------------------------------------------------
    def _tv(self, text: str) -> str:
        """Translate $$variables in any text snippet. Returns translated text."""
        result, _ = translate_variables(text, self.flat_params)
        return result

    # ------------------------------------------------------------------
    def generate(self, taskflows, mappings, workflows,
                 project_meta=None, connections=None, params=None):
        for d in ("models/staging","models/intermediate","models/marts","seeds","macros","analyses"):
            (self.out / d).mkdir(parents=True, exist_ok=True)

        self._write_dbt_project(project_meta)
        self._write_profiles(connections)
        self._write_macros()

        all_meta: list[dict] = []
        all_sources: list[dict] = []

        for m in mappings:
            meta, srcs = self._write_mapping_models(m)
            all_meta.extend(meta); all_sources.extend(srcs)

        for tf in taskflows:
            meta = self._write_tf_model(tf)
            all_meta.extend(meta)
            all_sources.extend([{"name": _snake(s.task_name),
                                  "description": f"Raw for {s.task_name}"} for s in tf.steps])

        for wf in workflows:
            all_meta.extend(self._write_workflow_model(wf, params or {}))

        self._write_schema_yml(all_meta)
        self._write_sources_yml(all_sources)
        self._write_seed_ddl(taskflows, mappings, workflows)
        self._write_lineage_report(mappings, taskflows, workflows, project_meta, connections, params)

        print(f"\n  Generated dbt project -> {self.out.resolve()}")
        print(f"   models/  - {len(all_meta)} SQL model(s)")
        print(f"   seeds/   - raw DDL")

    # ------------------------------------------------------------------
    # dbt_project.yml  — injects all $$PARAM values as dbt vars
    # ------------------------------------------------------------------
    def _write_dbt_project(self, project_meta):
        proj = _snake(project_meta["name"]) if project_meta else "informatica_to_dbt"
        desc = f"# {project_meta.get('description','')}" if project_meta else ""
        profile_name = f"{self.tgt_dialect}_profile" if self.tgt_dialect != "duckdb" else "duckdb_local"

        # Build dbt vars block from $$PARAM catalog
        dbt_vars = vars_to_dbt_project_vars(self.flat_params)
        # Always include dialect vars
        dbt_vars.update({"src_dialect": self.src_dialect, "tgt_dialect": self.tgt_dialect,
                          "run_id": "0", "log_id": "0"})
        vars_block = "\n".join(f"  {k}: \"{v}\"" for k, v in sorted(dbt_vars.items()))

        _write(self.out / "dbt_project.yml", textwrap.dedent(f"""\
            name: {proj}
            version: "1.0.0"
            config-version: 2
            {desc}
            # Source dialect : {self.src_dialect}
            # Target dialect : {self.tgt_dialect}
            # dbt adapter    : {_DIALECT_PACKAGES[self.tgt_dialect]}

            profile: {profile_name}
            model-paths: ["models"]
            seed-paths:  ["seeds"]
            macro-paths: ["macros"]
            target-path: "target"
            clean-targets: ["target","dbt_packages"]

            vars:
            {vars_block}

            models:
              {proj}:
                staging:
                  +materialized: view
                  +schema: staging
                intermediate:
                  +materialized: ephemeral
                marts:
                  +materialized: table
                  +schema: marts
        """))
        _write(self.out / "packages.yml", textwrap.dedent(f"""\
            packages:
              - package: dbt-labs/dbt_utils
                version: [">=1.0.0","<2.0.0"]
              # dbt adapter: pip install {_DIALECT_PACKAGES[self.tgt_dialect]}
        """))
        _write(self.out / "INSTALL.md", textwrap.dedent(f"""\
            # Setup Instructions

            ## 1. Install dbt adapter
            ```bash
            pip install {_DIALECT_PACKAGES[self.tgt_dialect]}
            ```

            ## 2. Configure profiles.yml
            Copy `profiles.yml` to `~/.dbt/profiles.yml` and fill in your credentials.

            ## 3. Install dbt packages
            ```bash
            dbt deps
            ```

            ## 4. Run models
            ```bash
            dbt run
            dbt test
            dbt docs generate && dbt docs serve
            ```

            ## Source database  : `{self.src_dialect}`
            ## Target database  : `{self.tgt_dialect}`
        """))

    def _write_profiles(self, connections=None):
        stanza = _DIALECT_PROFILES.get(self.tgt_dialect, _DIALECT_PROFILES["duckdb"])
        conn_comment = ""
        if connections:
            conn_comment = "\n# IDMC/PC connections: " + ", ".join(c["name"] for c in connections[:6])
        _write(self.out / "profiles.yml", stanza + conn_comment + "\n")
        if self.src_dialect != self.tgt_dialect:
            src_stanza = _DIALECT_PROFILES.get(self.src_dialect, _DIALECT_PROFILES["duckdb"])
            _write(self.out / "source_profiles.yml",
                   f"# Source database profile ({self.src_dialect})\n" + src_stanza)

    def _write_macros(self):
        _write(self.out / "macros" / "run_ics_task.sql", textwrap.dedent("""\
            {% macro run_ics_task(task_name, guid, task_type='MCT') %}
                select '{{ task_name }}' as object_name,
                    {{ var('run_id','0') }}::bigint as run_id,
                    {{ var('log_id','0') }}::bigint as log_id,
                    '{{ guid }}' as task_id, 'SUCCEEDED' as task_status,
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
            {% macro iif(cond, t, f) %}CASE WHEN {{ cond }} THEN {{ t }} ELSE {{ f }} END{% endmacro %}
        """))
        _write(self.out / "macros" / "scd2_merge.sql", textwrap.dedent("""\
            {% macro scd2_merge(target_table, source_ref, key_cols, hist_cols,
                                eff_from_col='eff_from_dt', eff_to_col='eff_to_dt',
                                curr_flag_col='curr_indc') %}
            UPDATE {{ target_table }} tgt
            SET    {{ eff_to_col }} = (select min({{ eff_from_col }}) from {{ source_ref }} src
                                        where {{ key_cols | map('tgt.'~x) | join(' AND ') }}),
                   {{ curr_flag_col }} = 0
            WHERE  {{ curr_flag_col }} = 1
              AND  EXISTS (select 1 from {{ source_ref }} src
                           where {{ key_cols | map('src.'~x~' = tgt.'~x) | join(' AND ') }}
                             and ({{ hist_cols | map('src.'~x~' <> tgt.'~x) | join(' OR ') }}));
            INSERT INTO {{ target_table }}
            select src.* from {{ source_ref }} src
            where not exists (select 1 from {{ target_table }} tgt
                              where {{ key_cols | map('src.'~x~' = tgt.'~x) | join(' AND ') }}
                                and {{ curr_flag_col }} = 1
                                and ({{ hist_cols | map('src.'~x~' = tgt.'~x) | join(' AND ') }}));
            {% endmacro %}
        """))
        # Macro to expose any remaining $$VAR at runtime
        _write(self.out / "macros" / "infa_var.sql", textwrap.dedent("""\
            {#
              infa_var: resolve an Informatica $$VARIABLE at dbt runtime.
              Usage: {{ infa_var('MY_PARAM', 'default_value') }}
              Maps to: {{ var('my_param', 'default_value') }}
            #}
            {% macro infa_var(param_name, default='') %}
                {{ var(param_name | lower, default) }}
            {% endmacro %}
        """))

    # ------------------------------------------------------------------
    # Core: one model per source->target pair
    # ------------------------------------------------------------------
    def _write_mapping_models(self, m: Mapping) -> tuple[list[dict], list[dict]]:
        meta_list: list[dict] = []
        all_sources: list[dict] = []

        base_name = f"{'mplt' if m.is_mapplet else 'stg'}_{_snake(m.name)}"
        base_sql  = self._mapping_base_sql(m, base_name)
        _write(self.out / "models" / "staging" / f"{base_name}.sql", base_sql)
        out_cols = self._output_columns(m)
        meta_list.append({"name": base_name,
                           "description": m.description or f"Base CTE model for mapping '{m.name}' (folder: {m.folder}).",
                           "columns": out_cols, "layer": "staging"})

        for inst_name, inst in m.instances.items():
            t = m.transformations.get(inst.transformation_name)
            if t and t.ttype in ("Input Transformation","Source Qualifier","Source Definition"):
                tbl = t.attributes.get("Table Name", t.attributes.get("Source table name", inst_name))
                all_sources.append({"name": _snake(tbl), "description": f"Source for {m.name}/{inst_name}"})

        for idx, pair in enumerate(m.source_target_pairs):
            pair_name = (f"{'mart' if pair.is_scd2 else 'stg'}_{_snake(m.name)}"
                         f"__{_snake(pair.source_table)}_to_{_snake(pair.target_table)}")
            if len(pair_name) > 100:
                pair_name = f"stg_{_snake(m.name)}__pair_{idx+1}"
            if pair.is_scd2:
                sql = self._scd2_model_sql(m, pair, pair_name)
                layer = "marts"
            else:
                sql = self._pair_model_sql(m, pair, pair_name, base_name)
                layer = "staging"
            subdir = "marts" if layer == "marts" else "staging"
            _write(self.out / "models" / subdir / f"{pair_name}.sql", sql)
            meta_list.append({
                "name": pair_name,
                "description": (f"{'SCD2 ' if pair.is_scd2 else ''}Source->Target: "
                                 f"`{pair.source_table}` -> `{pair.target_table}` "
                                 f"in mapping `{m.name}`."),
                "columns": out_cols, "layer": layer,
            })

        return meta_list, all_sources

    def _mapping_base_sql(self, m: Mapping, model_name: str) -> str:
        ordered  = m.topo_sort()
        upstream: dict[str, list[Connector]] = {n: [] for n in m.instances}
        for c in m.connectors:
            if c.to_instance in upstream:
                upstream[c.to_instance].append(c)

        header = textwrap.dedent(f"""\
            {{# -----------------------------------------------------------
               Model   : {model_name}
               Mapping : {m.name}  ({'Mapplet' if m.is_mapplet else 'Mapping'})
               Folder  : {m.folder}
               Desc    : {m.description or 'n/a'}
               S->T pairs: {len(m.source_target_pairs)}
            ----------------------------------------------------------- #}}
            {{{{ config(materialized='view') }}}}
        """)

        ctes = [cte for n in ordered
                if (inst := m.instances.get(n))
                and (t := m.transformations.get(inst.transformation_name)
                          or Transformation(name=inst.transformation_name,
                                            ttype=inst.transformation_type or "Unknown"))
                for cte in [self._instance_to_cte(n, inst, t, upstream, m)]
                if cte]

        if not ctes:
            return header + "\nselect 1 as placeholder\n"

        final = next(
            (n for n in reversed(ordered)
             if m.instances.get(n)
             and m.transformations.get(m.instances[n].transformation_name)
             and m.transformations[m.instances[n].transformation_name].ttype
             in ("Output Transformation","Target Definition")),
            ordered[-1] if ordered else "placeholder"
        )
        return header + "\nwith\n\n" + ",\n\n".join(ctes) + f"\n\nselect * from {_snake(final)}\n"

    def _pair_model_sql(self, m: Mapping, pair: SourceTargetPair,
                         model_name: str, base_ref: str) -> str:
        upd = pair.update_strategy.upper()
        if "DD_DELETE" in upd:
            where = "-- DELETE strategy: filter rows flagged for deletion"
            comment = "-- DuckDB: implement DELETE via MERGE or soft-delete flag"
        elif "DD_UPDATE" in upd:
            where = "-- UPDATE strategy"
            comment = "-- DuckDB: implement via MERGE INTO or INSERT ... ON CONFLICT"
        else:
            where = ""; comment = ""
        upstream_instances = ", ".join(f"`{n}`" for n in pair.transformations)
        return textwrap.dedent(f"""\
            {{# -----------------------------------------------------------
               Model   : {model_name}
               Source  : {pair.source_table}
               Target  : {pair.target_table}
               Path    : {upstream_instances}
               Strategy: {pair.update_strategy}
            ----------------------------------------------------------- #}}
            {{{{ config(materialized='table') }}}}

            with base as (
                select * from {{{{ ref('{base_ref}') }}}}
            )

            {comment}
            select *
            from base
            {where}
            -- Target: {pair.target_table}
        """)

    def _scd2_model_sql(self, m: Mapping, pair: SourceTargetPair, model_name: str) -> str:
        key_cols  = pair.scd_key_cols or ["id"]
        hist_cols = pair.scd_hist_cols or ["updated_at"]
        base_ref  = f"stg_{_snake(m.name)}"
        ordered   = m.topo_sort()
        upstream: dict[str, list[Connector]] = {n: [] for n in m.instances}
        for c in m.connectors:
            if c.to_instance in upstream:
                upstream[c.to_instance].append(c)
        path_set = set(pair.transformations)
        ctes = [cte for n in ordered
                if n in path_set
                and (inst := m.instances.get(n))
                and (t := m.transformations.get(inst.transformation_name))
                for cte in [self._instance_to_cte(n, inst, t, upstream, m)]
                if cte]
        cte_sql = (",\n\n".join(ctes) + ",\n\n") if ctes else ""
        key_list = ", ".join(key_cols); hist_list = ", ".join(hist_cols)
        return textwrap.dedent(f"""\
            {{# -----------------------------------------------------------
               Model   : {model_name}  [SCD TYPE 2]
               Source  : {pair.source_table}
               Target  : {pair.target_table}
               Keys    : {key_list}
               History : {hist_list}
            ----------------------------------------------------------- #}}
            {{{{ config(
                materialized = 'incremental',
                unique_key   = [{', '.join(repr(k) for k in key_cols)}],
                incremental_strategy = 'merge',
            ) }}}}

            with
            {cte_sql}
            source_data as (
                select * from {{{{ ref('{base_ref}') }}}}
                {{%- if is_incremental() %}}
                where updated_at > (select max(eff_from_dt) from {{{{ this }}}})
                {{%- endif %}}
            ),
            classified as (
                select
                    src.*,
                    case
                        when tgt.{key_cols[0] if key_cols else 'id'} is null then 'NEW'
                        when {" or ".join(f"src.{c} <> tgt.{c}" for c in hist_cols) if hist_cols else "false"} then 'CHANGED'
                        else 'UNCHANGED'
                    end as _row_action,
                    current_timestamp   as eff_from_dt,
                    '9999-12-31'::date  as eff_to_dt,
                    1                   as curr_indc
                from source_data src
                left join {{{{ this }}}} tgt
                    on {" and ".join(f"src.{k} = tgt.{k}" for k in key_cols) if key_cols else "false"}
                    and tgt.curr_indc = 1
            )
            select * from classified
            where _row_action in ('NEW','CHANGED')
        """)

    # ------------------------------------------------------------------
    # CTE builder — $$variables translated in every attribute
    # ------------------------------------------------------------------
    def _instance_to_cte(self, inst_name, inst, t, upstream, m) -> str:
        n    = _snake(inst_name)
        ups  = upstream.get(inst_name, [])
        up_n = list(dict.fromkeys(_snake(c.from_instance) for c in ups))
        fc   = up_n[0] if up_n else "/* missing upstream */"

        # ------ Source / Input ------
        if t.ttype in ("Input Transformation","Source Qualifier","Source Definition"):
            cols = ", ".join(f.name.lower() for f in t.outputs) or "*"
            # Translate $$PARAM in SQL override and Source Filter
            sq_raw = t.attributes.get("Sql Query", t.attributes.get("Source Filter",""))
            sq_override = self.xp.transpile(sq_raw)   # transpile() calls translate_variables internally
            src = self._tv(t.attributes.get("Table Name",
                           t.attributes.get("Source table name", inst_name)))
            src_ref = f"{{{{ source('raw', '{_snake(src)}') }}}}"
            if sq_override and len(sq_override) > 5:
                return (f"{n} as (\n    -- Source Qualifier SQL override\n"
                        f"    {sq_override}\n)")
            # User-defined join
            ud_join = self._tv(t.attributes.get("User Defined Join","")).strip()
            join_clause = f"\n    -- join: {ud_join}" if ud_join else ""
            return f"{n} as (\n    select {cols} from {src_ref}{join_clause}\n)"

        # ------ Target / Output ------
        if t.ttype in ("Output Transformation","Target Definition"):
            if not up_n: return ""
            col_map = {c.to_field: c.from_field for c in ups}
            cols = ", ".join(f"{col_map.get(f.name, f.name)} as {f.name.lower()}" for f in t.inputs)
            return f"{n} as (\n    select {cols}\n    from {up_n[0]}\n)"

        # ------ Expression ------
        if t.ttype == "Expression":
            pass_map = {c.to_field: c.from_field for c in ups}
            parts  = [f"        {pass_map.get(f.name, f.name)} as {f.name.lower()}" for f in t.inputs]
            parts += [f"        {self.xp.transpile(f.expression)} as {f.name.lower()}  -- local"
                      for f in t.locals]
            parts += [f"        {self.xp.transpile(f.expression) or f.name} as {f.name.lower()}"
                      for f in t.outputs]
            return (f"{n} as (\n    select\n        "
                    + "\n        ,".join(parts)
                    + f"\n    from {fc}\n)")

        # ------ Lookup ------
        if t.ttype == "Lookup Procedure":
            lkp_table = self._tv(t.lookup_table) or "unknown_lookup"
            condition  = self.xp.transpile(t.lookup_condition)
            ret_cols   = [f.name.lower() for f in t.fields
                          if "RETURN" in f.port_type or ("OUTPUT" in f.port_type and "LOOKUP" in f.port_type)]
            ret_sql    = ", ".join(f"lkp.{c}" for c in ret_cols) or "lkp.*"
            # Translate $$PARAM in lookup SQL override
            lkp_sql_raw = t.lookup_sql
            if lkp_sql_raw:
                lkp_src = f"(\n        {self.xp.transpile(lkp_sql_raw).strip()}\n    )"
            else:
                lkp_src = f"{{{{ source('raw', '{_snake(lkp_table)}') }}}}"
            conn  = self._tv(t.attributes.get("Connection Information",""))
            cache = t.attributes.get("Lookup caching enabled","NO")
            return (f"{n} as (\n"
                    f"    -- Lookup: {lkp_table}  conn:{conn}  cache:{cache}\n"
                    f"    select src.*, {ret_sql}\n    from {fc} src\n"
                    f"    left join {lkp_src} lkp\n"
                    f"        on {condition or '/* add condition */'}\n)")

        # ------ Filter ------
        if t.ttype == "Filter":
            cond = self.xp.transpile(t.filter_condition) or "TRUE"
            return f"{n} as (\n    select * from {fc}\n    where {cond}\n)"

        # ------ Joiner ------
        if t.ttype == "Joiner":
            d2    = up_n[1] if len(up_n) > 1 else "/* detail_src */"
            jc    = self.xp.transpile(t.attributes.get("Join Condition","TRUE"))
            jtype = t.attributes.get("Join Type","Normal").upper()
            sjoin = {"NORMAL":"INNER JOIN","MASTER OUTER":"LEFT JOIN",
                     "DETAIL OUTER":"RIGHT JOIN","FULL OUTER":"FULL OUTER JOIN"}.get(jtype,"INNER JOIN")
            return (f"{n} as (\n    -- Joiner ({jtype})\n"
                    f"    select m.*, d.*\n    from {fc} m\n"
                    f"    {sjoin} {d2} d on {jc}\n)")

        # ------ Aggregator ------
        if t.ttype == "Aggregator":
            gc    = [f.name.lower() for f in t.inputs if not f.expression]
            ag    = [f"        {self.xp.transpile(f.expression)} as {f.name.lower()}"
                     for f in t.outputs if f.expression]
            group = f"    group by {', '.join(gc)}" if gc else ""
            return (f"{n} as (\n    select\n        {', '.join(gc)}"
                    + (("\n        ," + "\n        ,".join(ag)) if ag else "")
                    + f"\n    from {fc}\n    {group}\n)")

        # ------ Router ------
        if t.ttype == "Router":
            ctes_out = []
            for grp_name, filter_key in [(k.replace("__group_filter_",""), v)
                                          for k, v in t.attributes.items()
                                          if k.startswith("__group_filter_")]:
                cond = self.xp.transpile(filter_key) or "TRUE"
                ctes_out.append(f"{n}_{_snake(grp_name)} as (\n    select * from {fc}\n    where {cond}\n)")
            ctes_out.append(f"{n}_default as (\n    select * from {fc}\n)")
            return ",\n\n".join(ctes_out)

        # ------ Sequence Generator ------
        if t.ttype == "Sequence Generator":
            start = t.seq_start; inc = t.seq_increment
            out_col = t.outputs[0].name.lower() if t.outputs else "nextval"
            return (f"{n} as (\n    -- Sequence: start={start} increment={inc}\n"
                    f"    select *, {start} + (row_number() over () - 1) * {inc} as {out_col}\n"
                    f"    from {fc}\n)")

        # ------ Rank ------
        if t.ttype == "Rank":
            rank_col = t.rank_by or (t.inputs[0].name.lower() if t.inputs else "1")
            rank_expr = self.xp.transpile(rank_col)
            top_n = t.rank_count
            grp_cols = [f.name.lower() for f in t.inputs
                        if not f.expression and f.name.lower() != _snake(rank_col)]
            part_sql = f"partition by {', '.join(grp_cols)}" if grp_cols else ""
            return (f"{n}_ranked as (\n    select *,\n"
                    f"           rank() over ({part_sql} order by {rank_expr} desc) as _rnk\n"
                    f"    from {fc}\n),\n"
                    f"{n} as (\n    select * from {n}_ranked where _rnk <= {top_n}\n)")

        # ------ Update Strategy ------
        if t.ttype == "Update Strategy":
            expr = self.xp.transpile(t.update_strategy)
            return (f"{n} as (\n    -- Update Strategy: {t.update_strategy}\n"
                    f"    select *,\n           {expr} as _update_strategy_flag\n"
                    f"    from {fc}\n)")

        # ------ Stored Procedure ------
        if t.ttype == "Stored Procedure":
            ic   = ", ".join(f.name.lower() for f in t.inputs)
            oc   = ", ".join(f"null::{f.duckdb_type} as {f.name.lower()}" for f in t.outputs)
            conn = self._tv(t.attributes.get("Connection Information","$Target"))
            return (f"{n} as (\n    -- SP: {t.sp_name}({ic})  conn:{conn}\n"
                    f"    -- TODO: implement as DuckDB macro / UDF\n"
                    f"    select src.*, {oc or 'null as proc_result'}\n"
                    f"    from {fc} src\n)")

        # ------ Normalizer ------
        if t.ttype in ("Normalizer","Normalizer Transformation"):
            occur = t.normalizer_occur
            unions = []
            for i in range(1, occur + 1):
                cols = ", ".join(
                    f"{f.name.lower()}_{i} as {f.name.lower()}"
                    if any(f.name.lower()+f"_{j}" in [ff.name.lower() for ff in t.inputs]
                           for j in range(1, occur+1))
                    else f.name.lower()
                    for f in t.inputs
                )
                unions.append(f"    select {cols or '*'}, {i} as occurrence_num from {fc}")
            return f"{n} as (\n" + "\n    union all\n".join(unions) + "\n)"

        # ------ Mapplet instance ------
        if t.ttype == "Mapplet":
            ref = f"mplt_{_snake(inst.transformation_name)}"
            return (f"{n} as (\n    -- Mapplet: {inst.transformation_name}\n"
                    f"    select * from {{{{ ref('{ref}') }}}}\n"
                    f"    -- TODO: pass inputs from {fc}\n)")

        # ------ Fallback ------
        return f"{n} as (\n    -- {t.ttype}: {inst_name} (pass-through)\n    select * from {fc}\n)"

    # ------------------------------------------------------------------
    def _ics_cols(self) -> dict[str, str]:
        src_map = {
            "Object_Name":"string",    "Run_Id":"bigint",    "Log_Id":"bigint",
            "Task_Id":"string",        "Task_Status":"string",
            "Success_Source_Rows":"bigint","Failed_Source_Rows":"bigint",
            "Success_Target_Rows":"bigint","Failed_Target_Rows":"bigint",
            "Start_Time":"date/time",  "End_Time":"date/time",
            "Error_Message":"string",  "TotalTransErrors":"bigint",
            "FirstErrorCode":"string",
        }
        return {col: dialect_type(infa_t, self.tgt_dialect) for col, infa_t in src_map.items()}

    def _output_columns(self, m: Mapping) -> list[dict]:
        for inst_name in reversed(m.topo_sort()):
            inst = m.instances.get(inst_name)
            if not inst: continue
            t = m.transformations.get(inst.transformation_name)
            if t and t.ttype in ("Output Transformation","Target Definition"):
                return [{"name": f.name.lower(), "dtype": f.native_type(self.tgt_dialect),
                          "description": f.description} for f in t.inputs]
        for inst_name in reversed(m.topo_sort()):
            inst = m.instances.get(inst_name)
            if not inst: continue
            t = m.transformations.get(inst.transformation_name)
            if t and t.outputs:
                return [{"name": f.name.lower(), "dtype": f.native_type(self.tgt_dialect),
                          "description": f.description} for f in t.outputs]
        return []

    # ------------------------------------------------------------------
    # Taskflow models
    # ------------------------------------------------------------------
    def _write_tf_model(self, tf: Taskflow) -> list[dict]:
        meta: list[dict] = []
        ics = self._ics_cols()
        for step in tf.steps:
            mn  = f"stg_{_snake(step.task_name)}"
            cds = ",\n    ".join(f"{c}  as {c.lower()}" for c in ics)
            sql = textwrap.dedent(f"""\
                {{# Task: {step.task_name} | TF: {tf.name} | dialect: {self.tgt_dialect} #}}
                {{{{ config(materialized='view') }}}}
                with ics_raw as (
                    {{{{ run_ics_task(task_name='{step.task_name}', guid='{step.guid}',
                                task_type='{step.task_type}') }}}}
                ),
                typed as (select {cds} from ics_raw)
                select * from typed
            """)
            _write(self.out / "models" / "staging" / f"{mn}.sql", sql)
            meta.append({"name": mn,
                          "description": f"Task '{step.task_name}' from taskflow '{tf.name}'.",
                          "columns": [{"name": c.lower(), "dtype": d, "description": ""}
                                      for c, d in ics.items()], "layer": "staging"})
        orch = f"int_{_snake(tf.name)}_audit"
        unions = [f"    select {i} as step_order,'{s.task_name}' as task_name,* "
                  f"from {{{{ ref('stg_{_snake(s.task_name)}') }}}}"
                  for i, s in enumerate(tf.steps, 1)] or ["    select null,null"]
        diff = self._dsql["datediff"].format("start_time","end_time")
        _write(self.out / "models" / "intermediate" / f"{orch}.sql", textwrap.dedent(f"""\
            {{{{ config(materialized='table') }}}}
            with steps as ({"    union all".join(unions)})
            select step_order,task_name,task_status,
                   success_source_rows,failed_source_rows,success_target_rows,failed_target_rows,
                   total_trans_errors,first_error_code,error_message,start_time,end_time,
                   {diff} as duration_seconds,
                   run_id,log_id,task_id,object_name
            from steps order by step_order
        """))
        meta.append({"name": orch, "description": f"Audit for taskflow '{tf.name}'.",
                      "columns": [{"name": c,"dtype":"varchar","description":""}
                                  for c in ["step_order","task_name","task_status","duration_seconds"]],
                      "layer": "intermediate"})
        return meta

    # ------------------------------------------------------------------
    # Workflow model
    # ------------------------------------------------------------------
    def _write_workflow_model(self, wf: Workflow, params: dict) -> list[dict]:
        model_name = f"int_{_snake(wf.name)}_wf_audit"
        flat: dict[str, str] = {}
        for sec_vals in params.values():
            flat.update(sec_vals)
        unions = []
        for i, sess in enumerate(wf.sessions, 1):
            src_c = self._resolve_param(next(iter(sess.source_connections.values()),""), flat)
            tgt_c = self._resolve_param(next(iter(sess.target_connections.values()),""), flat)
            ref   = f"stg_{_snake(sess.mapping_name)}" if sess.mapping_name else "/* no mapping */"
            unions.append(
                f"    select {i} as step_order, '{sess.name}' as session_name,\n"
                f"           '{sess.mapping_name}' as mapping_name,\n"
                f"           '{src_c}' as source_connection,\n"
                f"           '{tgt_c}' as target_connection,\n"
                f"           current_timestamp as created_at\n"
                f"    -- ref: {{{{ ref('{ref}') }}}}"
            )
        union_sql = "\n    union all\n".join(unions) or "    select null as step_order"
        sql = textwrap.dedent(f"""\
            {{# Workflow: {wf.name} | Folder: {wf.folder} #}}
            {{{{ config(materialized='table') }}}}
            with sessions as (
            {union_sql}
            )
            select step_order,session_name,mapping_name,source_connection,target_connection,created_at
            from sessions order by step_order
        """)
        _write(self.out / "models" / "intermediate" / f"{model_name}.sql", sql)
        return [{"name": model_name,
                 "description": wf.description or f"Audit for workflow '{wf.name}'.",
                 "columns": [{"name": c,"dtype":"varchar","description":""}
                              for c in ["step_order","session_name","mapping_name",
                                        "source_connection","target_connection"]],
                 "layer": "intermediate"}]

    @staticmethod
    def _resolve_param(val: str, flat: dict) -> str:
        if not val: return val
        # Use translate_variables for $$VAR tokens in connection names
        result, _ = translate_variables(val, flat)
        return result

    # ------------------------------------------------------------------
    # schema.yml
    # ------------------------------------------------------------------
    def _write_schema_yml(self, meta: list[dict]):
        lines = ["version: 2","","models:"]
        for m in meta:
            lines += [f"  - name: {m['name']}","    description: >",
                      f"      {m['description']}","    columns:"]
            for col in m.get("columns",[]):
                cn = col["name"] if isinstance(col,dict) else col.lower()
                lines.append(f"      - name: {cn}")
                if cn in ("run_id","log_id"):
                    lines += ["        tests:","          - not_null"]
                if cn == "task_status":
                    lines += ["        tests:","          - accepted_values:",
                              "              values: ['SUCCEEDED','FAILED','RUNNING','STOPPED']"]
            lines.append("")
        _write(self.out / "models" / "schema.yml", "\n".join(lines))

    # ------------------------------------------------------------------
    # sources.yml
    # ------------------------------------------------------------------
    def _write_sources_yml(self, sources: list[dict]):
        seen: set[str] = set()
        lines = ["version: 2","","sources:",
                 "  - name: raw",
                 f"    description: 'Raw landing schema ({self.src_dialect})'",
                 "    meta:",
                 f"      src_dialect: {self.src_dialect}",
                 f"      tgt_dialect: {self.tgt_dialect}",
                 "    database: dev",
                 "    schema: raw",
                 "    tables:"]
        for s in sources:
            if s["name"] in seen: continue
            seen.add(s["name"])
            lines += [f"      - name: {s['name']}",
                      f"        description: \"{s['description']}\""]
        _write(self.out / "models" / "sources.yml", "\n".join(lines))

    # ------------------------------------------------------------------
    # Seed DDL
    # ------------------------------------------------------------------
    def _write_seed_ddl(self, taskflows, mappings, workflows):
        ics     = self._ics_cols()
        src_d   = self.src_dialect
        tgt_d   = self.tgt_dialect
        ts_type = dialect_type("date/time", src_d)
        ics_ddl = ",\n    ".join(f"{c.lower()}  {t}" for c, t in ics.items())
        blocks  = [f"-- Auto-generated DDL\n-- Source: {src_d}  |  Target: {tgt_d}\n\n",
                   "-- ===== SOURCE RAW TABLES =====\n"]
        for m in mappings:
            for inst_name, inst in m.instances.items():
                t = m.transformations.get(inst.transformation_name)
                if t and t.ttype in ("Input Transformation","Source Qualifier","Source Definition"):
                    tbl = t.attributes.get("Table Name", inst_name)
                    col_block = (",\n    ".join(f"{f.name.lower()}  {f.native_type(src_d)}"
                                                for f in t.outputs) or "id  integer")
                    blocks.append(f"-- Mapping: {m.name} / {inst_name}\n"
                                  f"CREATE TABLE IF NOT EXISTS raw.{_snake(tbl)} (\n"
                                  f"    {col_block},\n"
                                  f"    _loaded_at  {ts_type} default current_timestamp\n);\n")
        blocks.append("\n-- ===== TARGET TABLES =====\n")
        for m in mappings:
            for pair in m.source_target_pairs:
                tgt_inst  = m.instances.get(pair.target_instance)
                tgt_trans = m.transformations.get(tgt_inst.transformation_name) if tgt_inst else None
                col_block = (",\n    ".join(f"{f.name.lower()}  {f.native_type(tgt_d)}"
                                            for f in tgt_trans.inputs)
                             if tgt_trans else "id  integer")
                scd_extra = (f",\n    eff_from_dt  {dialect_type('date/time',tgt_d)} not null"
                             f",\n    eff_to_dt    {dialect_type('date/time',tgt_d)}"
                             f",\n    curr_indc    {dialect_type('integer',tgt_d)} default 1"
                             ) if pair.is_scd2 else ""
                blocks.append(f"-- Target: {pair.target_table}\n"
                              f"CREATE TABLE IF NOT EXISTS {_snake(pair.target_table)} (\n"
                              f"    {col_block}{scd_extra},\n"
                              f"    _dbt_loaded_at  {dialect_type('date/time',tgt_d)} default current_timestamp\n);\n")
        blocks.append("\n-- ===== TASKFLOW AUDIT TABLES =====\n")
        for tf in taskflows:
            for step in tf.steps:
                blocks.append(f"CREATE TABLE IF NOT EXISTS raw.{_snake(step.task_name)} (\n"
                              f"    {ics_ddl},\n"
                              f"    _loaded_at  {ts_type} default current_timestamp\n);\n")
        _write(self.out / "seeds" / "create_raw_tables.sql", "\n".join(blocks))

    # ------------------------------------------------------------------
    # Lineage report — includes $$variable resolution summary
    # ------------------------------------------------------------------
    def _write_lineage_report(self, mappings, taskflows, workflows,
                               project_meta, connections, params):
        lines = ["# Informatica -> dbt Lineage Report\n"]
        if project_meta:
            lines += [f"**Project:** {project_meta['name']}",
                      f"**Description:** {project_meta.get('description','')}",""]
        if connections:
            lines += ["## Connections\n","| Name | Type | Host | Database |",
                      "|------|------|------|----------|"]
            for c in connections:
                lines.append(f"| {c['name']} | {c.get('type','')} | {c.get('host','')} | {c.get('database','')} |")
            lines.append("")

        # $$Variable catalog
        if self.flat_params:
            lines += ["## $$Variable Resolution\n",
                      "| Informatica Variable | dbt Translation |",
                      "|----------------------|-----------------|"]
            for key, val in sorted(self.flat_params.items()):
                translated = translate_variable(key, self.flat_params)
                lines.append(f"| `{key}` | `{translated}` |")
            # Built-in system vars
            lines.append("")
            lines += ["### Built-in PC System Variables\n",
                      "| Variable | dbt Equivalent |",
                      "|----------|----------------|"]
            for k, v in sorted(_BUILTIN_VAR_MAP.items()):
                lines.append(f"| `{k}` | `{v}` |")
            lines.append("")

        lines.append("## Mappings\n")
        for m in mappings:
            ordered = m.topo_sort()
            dag = " -> ".join(
                f"`{n}` ({(m.transformations.get(m.instances[n].transformation_name) or type('', (), {'ttype': m.instances[n].transformation_type or '?'})()).ttype if m.instances.get(n) else '?'})"
                for n in ordered
            )
            lines += [f"### `{m.name}` ({'Mapplet' if m.is_mapplet else 'Mapping'}) -- `{m.folder}`\n",
                      f"**DAG:** {dag}\n",
                      f"**Source->Target pairs ({len(m.source_target_pairs)}):**\n"]
            for pair in m.source_target_pairs:
                scd_note = " *[SCD2]*" if pair.is_scd2 else ""
                lines.append(f"  - `{pair.source_table}` -> `{pair.target_table}`"
                             f"  strategy:`{pair.update_strategy}`{scd_note}")
                if pair.scd_key_cols:
                    lines.append(f"    Keys: {', '.join(pair.scd_key_cols)}")
                if pair.scd_hist_cols:
                    lines.append(f"    History cols: {', '.join(pair.scd_hist_cols)}")
            lines.append("\n**Expressions:**\n")
            for tname, t in m.transformations.items():
                for f in t.fields:
                    if f.expression:
                        raw = f.expression.replace("\r\n"," ").replace("\n"," ")[:100]
                        dbt = self.xp.transpile(f.expression)[:100]
                        if raw != dbt:
                            lines += [f"  - `{tname}.{f.name}`",
                                      f"    - Infa   : `{raw}`",
                                      f"    - DuckDB : `{dbt}`"]
            lines.append("")
        lines.append("## Workflows\n")
        for wf in workflows:
            lines += [f"### `{wf.name}` -- `{wf.folder}`\n","**Sessions:**\n"]
            for i, s in enumerate(wf.sessions, 1):
                lines.append(f"  {i}. `{s.name}` -> mapping:`{s.mapping_name}`")
            lines.append("")
        lines.append("## Taskflows\n")
        for tf in taskflows:
            lines += [f"### `{tf.name}`\n","**Steps:**\n"]
            for i, s in enumerate(tf.steps, 1):
                lines.append(f"  {i}. `{s.task_name}` ({s.task_type}) GUID:`{s.guid}`")
            lines.append("")
        _write(self.out / "lineage_report.md", "\n".join(lines))


# ===========================================================================
# IDMC client
# ===========================================================================
@dataclass
class IDMCConfig:
    pod_url: str; token: str
    page_size: int = 50; max_retries: int = 3; retry_delay: float = 2.0
    @classmethod
    def from_args(cls, args) -> "IDMCConfig":
        pod_url = getattr(args,"pod_url",None) or os.environ.get("IDMC_POD_URL","")
        token   = getattr(args,"token",None)   or os.environ.get("IDMC_TOKEN","")
        if not pod_url or not token:
            raise ValueError("Provide --pod-url and --token, or set IDMC_POD_URL/IDMC_TOKEN.")
        return cls(pod_url=pod_url.rstrip("/"), token=token)

class IDMCClient:
    _D="/api/v3"; _P="/api/v2"
    def __init__(self, cfg: IDMCConfig):
        self.cfg=cfg
        self._h={"Authorization":f"Bearer {cfg.token}","Content-Type":"application/json","Accept":"application/json"}
    def _get(self, url, params=None):
        full = url if url.startswith("http") else f"{self.cfg.pod_url}{url}"
        if params: full += "?" + "&".join(f"{k}={v}" for k,v in params.items())
        for attempt in range(1, self.cfg.max_retries+1):
            try:
                if _HTTP=="httpx":
                    r=httpx.get(full,headers=self._h,timeout=60); r.raise_for_status(); return r.json()
                else:
                    req=urllib.request.Request(full,headers=self._h)
                    with urllib.request.urlopen(req,timeout=60) as r: return json.loads(r.read().decode())
            except Exception as exc:
                if attempt==self.cfg.max_retries: raise RuntimeError(f"GET {full}: {exc}") from exc
                time.sleep(self.cfg.retry_delay*attempt)
    def _get_bin(self, url):
        full=url if url.startswith("http") else f"{self.cfg.pod_url}{url}"
        h=dict(self._h); h["Accept"]="application/xml,*/*"
        for attempt in range(1, self.cfg.max_retries+1):
            try:
                if _HTTP=="httpx":
                    r=httpx.get(full,headers=h,timeout=120); r.raise_for_status(); return r.content
                else:
                    req=urllib.request.Request(full,headers=h)
                    with urllib.request.urlopen(req,timeout=120) as r: return r.read()
            except Exception as exc:
                if attempt==self.cfg.max_retries: raise RuntimeError(f"BinGET {full}: {exc}") from exc
                time.sleep(self.cfg.retry_delay*attempt)
    def _pag(self, url, extra=None):
        res,off=[],0
        while True:
            p={"limit":self.cfg.page_size,"skip":off}
            if extra: p.update(extra)
            page=self._get(url,p)
            items=page if isinstance(page,list) else page.get("items",page.get("data",[]))
            if not items: break
            res.extend(items)
            if len(items)<self.cfg.page_size: break
            off+=self.cfg.page_size
        return res
    def list_mappings(self,pid=None):
        p={"type":"MTT"}
        if pid: p["projectId"]=pid
        r=self._pag(f"{self._D}/mttasks",p); print(f"    IDMC: {len(r)} mapping(s)"); return r
    def list_mapplets(self,pid=None):
        p={"projectId":pid} if pid else {}
        r=self._pag(f"{self._D}/mapplets",p); print(f"    IDMC: {len(r)} mapplet(s)"); return r
    def export_xml(self,oid,otype):
        payload=json.dumps({"objects":[{"id":oid,"type":otype}],"options":{"exportFormat":"POWERMART"}}).encode()
        url=f"{self.cfg.pod_url}{self._D}/export"; h=dict(self._h); h["Accept"]="application/xml"
        if _HTTP=="httpx":
            r=httpx.post(url,content=payload,headers=h,timeout=120); r.raise_for_status()
            if r.status_code==202: return self._poll(r.headers.get("Location",""))
            return r.content
        else:
            req=urllib.request.Request(url,data=payload,headers=h,method="POST")
            with urllib.request.urlopen(req,timeout=120) as r: return r.read()
    def _poll(self,job_url,max_wait=300):
        for _ in range(max_wait//5):
            time.sleep(5); d=self._get(job_url)
            if d.get("status")=="SUCCESSFUL": return self._get_bin(d.get("downloadUrl",""))
            if d.get("status") in ("FAILED","ERROR"): raise RuntimeError(f"Export failed: {d}")
        raise TimeoutError("Export timed out")
    def list_taskflows(self,pid=None):
        p={"projectId":pid} if pid else {}
        r=self._pag(f"{self._D}/taskflows",p); print(f"    IDMC: {len(r)} taskflow(s)"); return r
    def export_tf_xml(self,tid): return self._get_bin(f"{self._D}/taskflows/{tid}/export")
    def get_tf(self,tid): return self._get(f"{self._D}/taskflows/{tid}")
    def list_connections(self):
        r=self._pag(f"{self._P}/connections"); print(f"    IDMC: {len(r)} connection(s)"); return r
    def get_conn(self,cid): return self._get(f"{self._P}/connections/{cid}")

class IDMCFetcher:
    def __init__(self, client: IDMCClient, out_dir: Path):
        self.client=client; self.out=out_dir
        self.raw=out_dir/"_idmc_raw"; self.raw.mkdir(parents=True,exist_ok=True)
    def fetch_all(self, pid=None):
        print("\n[IDMC] Connections..."); conns=self._fetch_conns()
        print("\n[IDMC] Mappings..."); mappings=self._fetch_mappings(pid)
        print("\n[IDMC] Taskflows..."); taskflows=self._fetch_tf(pid)
        return mappings, taskflows, conns
    def _fetch_conns(self):
        raw=self.client.list_connections(); cat=[]
        for c in raw:
            cid=c.get("id",""); cn=c.get("name",cid)
            try: d=self.client.get_conn(cid)
            except: d=c
            cat.append({"id":cid,"name":cn,"type":d.get("type",""),"host":d.get("host",""),
                         "port":str(d.get("port","")),"database":d.get("database",""),
                         "username":d.get("username","")})
            _write(self.raw/"connections"/f"{_snake(cn)}.json", json.dumps(d,indent=2))
        _write(self.out/"connections_catalog.json", json.dumps(cat,indent=2))
        return cat
    def _fetch_mappings(self, pid):
        pm=PowerMartParser(); all_m=[]
        for item in self.client.list_mappings(pid):
            oid,on=item.get("id",""),item.get("name","")
            print(f"    -> {on}")
            try:
                xb=self.client.export_xml(oid,"MTT")
                xp=self.raw/"mappings"/f"{_snake(on)}.xml"; xp.parent.mkdir(parents=True,exist_ok=True)
                xp.write_bytes(xb); ms,_=pm.parse_file(str(xp)); all_m.extend(ms)
            except Exception as e: print(f"       Warning: {e}")
        for item in self.client.list_mapplets(pid):
            oid,on=item.get("id",""),item.get("name","")
            try:
                xb=self.client.export_xml(oid,"MAPPLET")
                xp=self.raw/"mapplets"/f"{_snake(on)}.xml"; xp.parent.mkdir(parents=True,exist_ok=True)
                xp.write_bytes(xb); ms,_=pm.parse_file(str(xp)); all_m.extend(ms)
            except Exception as e: print(f"       Warning: {e}")
        return all_m
    def _fetch_tf(self, pid):
        tfp=InformaticaTaskflowParser(); tfs=[]
        for item in self.client.list_taskflows(pid):
            tid,tn=item.get("id",""),item.get("name","")
            try:
                xb=self.client.export_tf_xml(tid)
                xp=self.raw/"taskflows"/f"{_snake(tn)}.xml"; xp.parent.mkdir(parents=True,exist_ok=True)
                xp.write_bytes(xb); tf=tfp.parse_file(str(xp)); tfs.append(tf)
                print(f"    -> '{tf.name}' ({len(tf.steps)} steps)")
            except Exception as e: print(f"       Warning: {e}")
        return tfs


# ===========================================================================
# PowerCenter Fetcher (SOAP / DB / XML)
# ===========================================================================
class PCSOAPClient:
    def __init__(self, host, port, repo, user, password, use_https=False):
        self.base=f"{'https' if use_https else 'http'}://{host}:{port}"
        self.repo=repo; self.user=user; self.password=password; self._token=""
    def connect(self):
        try:
            import requests; self._sess=requests.Session()
        except ImportError:
            raise RuntimeError("pip install requests lxml")
        body=(f"<mws:Login><mws:RepositoryName>{self.repo}</mws:RepositoryName>"
              f"<mws:UserName>{self.user}</mws:UserName>"
              f"<mws:Password>{self.password}</mws:Password></mws:Login>")
        resp=self._post("Login",body)
        tok=resp.find(".//{*}SessionToken")
        self._token=tok.text if tok is not None else ""
        print(f"  [PC SOAP] Connected {self.base}")
    def _post(self, action, body):
        env=(f'<?xml version="1.0"?><soapenv:Envelope '
             f'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
             f'xmlns:mws="http://www.informatica.com/wsh">'
             f"<soapenv:Header/><soapenv:Body>{body}</soapenv:Body></soapenv:Envelope>")
        url=f"{self.base}/wsh/services/RepositoryService"
        hdrs={"Content-Type":"text/xml; charset=utf-8","SOAPAction":f'"{action}"'}
        if self._token: hdrs["Authorization"]=f"Basic {self._token}"
        r=self._sess.post(url,data=env.encode(),headers=hdrs,timeout=120); r.raise_for_status()
        root=ET.fromstring(r.content)
        body_el=root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
        return body_el[0] if body_el is not None and len(body_el) else root
    def list_folders(self):
        body=f"<mws:GetAllFolders><mws:SessionToken>{self._token}</mws:SessionToken></mws:GetAllFolders>"
        resp=self._post("GetAllFolders",body)
        return [el.text for el in resp.findall(".//{*}FolderName") if el.text]
    def export_folder_xml(self, folder):
        body=(f"<mws:ExportObjects><mws:SessionToken>{self._token}</mws:SessionToken>"
              f"<mws:ObjectsToExport><mws:FolderName>{folder}</mws:FolderName>"
              f"<mws:ObjectTypes>mapping</mws:ObjectTypes>"
              f"<mws:ObjectTypes>mapplet</mws:ObjectTypes>"
              f"<mws:ObjectTypes>workflow</mws:ObjectTypes>"
              f"</mws:ObjectsToExport>"
              f"<mws:DependencyOptions><mws:AddDependency>false</mws:AddDependency>"
              f"</mws:DependencyOptions></mws:ExportObjects>")
        resp=self._post("ExportObjects",body)
        el=resp.find(".//{*}ExportedObjects")
        return el.text.encode() if el is not None and el.text else ET.tostring(resp)
    def disconnect(self):
        try:
            self._post("Logout",f"<mws:Logout><mws:SessionToken>{self._token}</mws:SessionToken></mws:Logout>")
        except: pass


class PCFetcher:
    def __init__(self, args, out_dir: Path):
        self.args=args; self.out=out_dir
        self.raw=out_dir/"_pc_raw"; self.raw.mkdir(parents=True,exist_ok=True)
        self.pm=PowerMartParser(); self.par=ParameterFileParser()
    def fetch_all(self):
        mode=getattr(self.args,"pc_mode","xml")
        if mode=="soap": return self._soap()
        if mode=="db":   return self._db()
        return self._xml()
    def _soap(self):
        client=PCSOAPClient(self.args.pc_host,int(self.args.pc_port or 7333),
                             self.args.pc_repo,self.args.pc_user,self.args.pc_password,
                             getattr(self.args,"pc_https",False))
        client.connect()
        all_m,all_w=[],[]
        try:
            for folder in client.list_folders():
                try:
                    xb=client.export_folder_xml(folder)
                    xp=self.raw/f"{_snake(folder)}.xml"; xp.write_bytes(xb)
                    ms,wfs=self.pm.parse_file(str(xp)); all_m.extend(ms); all_w.extend(wfs)
                except Exception as e: print(f"    Warning {folder}: {e}")
        finally: client.disconnect()
        return all_m, all_w, self._infer_conns(all_w), self._params()
    def _db(self):
        try: from sqlalchemy import create_engine, text
        except ImportError: raise RuntimeError("pip install sqlalchemy cx_Oracle")
        engine=create_engine(self.args.pc_db_url)
        def q(sql, p=None):
            with engine.connect() as conn:
                r=conn.execute(text(sql), p or {})
                cols=list(r.keys())
                return [dict(zip(cols,row)) for row in r]
        mapping_rows=q("""SELECT m.MAPPING_ID,m.MAPPING_NAME,m.COMMENTS,
            s.SUBJ_NAME AS FOLDER_NAME,
            CASE WHEN m.IS_MAPPLET=1 THEN 'MAPPLET' ELSE 'MAPPING' END AS OBJ_TYPE
            FROM OPB_MAPPING m JOIN OPB_SUBJECT s ON s.SUBJ_ID=m.SUBJECT_ID""")
        all_m=[]
        for row in mapping_rows:
            mid=row["MAPPING_ID"]
            m=Mapping(name=row["MAPPING_NAME"],folder=row["FOLDER_NAME"],
                      description=row.get("COMMENTS",""),
                      is_mapplet=(row["OBJ_TYPE"]=="MAPPLET"))
            wids={}
            for wr in q("SELECT WIDGET_ID,WIDGET_NAME,WIDGET_TYPE_NAME,REUSABLE,DESCRIPTION FROM OPB_WIDGET WHERE MAPPING_ID=:mid",{"mid":mid}):
                wid=wr["WIDGET_ID"]; wn=wr["WIDGET_NAME"]; wids[wid]=wn
                t=Transformation(name=wn,ttype=wr.get("WIDGET_TYPE_NAME",""),
                                  description=wr.get("DESCRIPTION",""),reusable=bool(wr.get("REUSABLE",0)))
                m.transformations[wn]=t; m.instances[wn]=MappingInstance(wn,wn,t.ttype)
            for fr in q("""SELECT WIDGET_ID,FIELD_NAME,DATATYPE,PORTTYPE,DEFAULT_VALUE,
                DESCRIPTION,PRECISION,SCALE,EXPRESSION FROM OPB_WIDGET_FIELD
                WHERE WIDGET_ID IN (SELECT WIDGET_ID FROM OPB_WIDGET WHERE MAPPING_ID=:mid)""",{"mid":mid}):
                wn=wids.get(fr["WIDGET_ID"])
                if wn and wn in m.transformations:
                    m.transformations[wn].fields.append(TransformField(
                        name=fr["FIELD_NAME"],datatype=fr.get("DATATYPE","string"),
                        port_type=fr.get("PORTTYPE","INPUT"),expression=fr.get("EXPRESSION","") or "",
                        precision=int(fr.get("PRECISION",4000) or 4000),scale=int(fr.get("SCALE",0) or 0),
                        default_value=fr.get("DEFAULT_VALUE","") or "",description=fr.get("DESCRIPTION","") or ""))
            for ar in q("""SELECT WIDGET_ID,ATTR_NAME,ATTR_VALUE FROM OPB_WIDGET_ATTR
                WHERE WIDGET_ID IN (SELECT WIDGET_ID FROM OPB_WIDGET WHERE MAPPING_ID=:mid)""",{"mid":mid}):
                wn=wids.get(ar["WIDGET_ID"])
                if wn and wn in m.transformations:
                    m.transformations[wn].attributes[ar["ATTR_NAME"]]=ar["ATTR_VALUE"] or ""
            for lr in q("""SELECT FROM_WIDGET_ID,FROM_FIELD_NAME,TO_WIDGET_ID,TO_FIELD_NAME
                FROM OPB_LINK WHERE MAPPING_ID=:mid""",{"mid":mid}):
                from_inst  = wids.get(lr["FROM_WIDGET_ID"], str(lr["FROM_WIDGET_ID"]))
                to_inst    = wids.get(lr["TO_WIDGET_ID"],   str(lr["TO_WIDGET_ID"]))
                from_field = lr["FROM_FIELD_NAME"] or ""
                to_field   = lr["TO_FIELD_NAME"]   or ""
                for inst_name, wid_key in ((from_inst, lr["FROM_WIDGET_ID"]),
                                           (to_inst,   lr["TO_WIDGET_ID"])):
                    if inst_name and inst_name not in m.instances:
                        m.instances[inst_name] = MappingInstance(
                            name=inst_name, transformation_name=inst_name,
                            transformation_type="Unknown", reusable=True)
                if from_inst and to_inst:
                    m.connectors.append(Connector(from_instance=from_inst, from_field=from_field,
                                                   to_instance=to_inst,     to_field=to_field))
            m.detect_source_target_pairs()
            if m.transformations: all_m.append(m)
        conns=[{"id":str(r.get("OBJECT_ID","")),"name":r.get("OBJECT_NAME",""),
                 "type":r.get("OBJECT_TYPE_NAME",""),"host":r.get("SERVER_NAME",""),
                 "port":str(r.get("PORT_NO","")),"database":r.get("DATABASE_NAME","")}
               for r in q("SELECT OBJECT_ID,OBJECT_NAME,OBJECT_TYPE_NAME,SERVER_NAME,PORT_NO,DATABASE_NAME FROM OPB_CNX")]
        return all_m, [], conns, self._params()
    def _xml(self):
        xml_path=getattr(self.args,"xml",None) or "."
        files=collect_files(xml_path,(".xml",))
        all_m,all_w=[],[]
        for f in files:
            try:
                ms,wfs=self.pm.parse_file(f); all_m.extend(ms); all_w.extend(wfs)
            except Exception as e: print(f"    Warning {f}: {e}")
        return all_m, all_w, self._infer_conns(all_w), self._params()
    def _params(self):
        pp=getattr(self.args,"pc_params",None)
        if not pp: return {}
        try:
            cat=self.par.parse_dir(pp) if Path(pp).is_dir() else self.par.parse_file(pp)
            _write(self.out/"param_catalog.json", json.dumps(cat,indent=2))
            return cat
        except Exception as e:
            print(f"  Warning param load: {e}"); return {}
    @staticmethod
    def _infer_conns(workflows):
        seen=set(); conns=[]
        for wf in workflows:
            for s in wf.sessions:
                for n in list(s.source_connections.values())+list(s.target_connections.values()):
                    if n and n not in seen:
                        seen.add(n); conns.append({"id":n,"name":n,"type":"inferred","host":"","database":""})
        return conns


# ===========================================================================
# CLI
# ===========================================================================
def main():
    ap = argparse.ArgumentParser(
        description="Informatica (IDMC/PowerCenter) -> dbt/DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              python informatica_to_dbt.py --xml ./exports/ --json project.json
              python informatica_to_dbt.py --xml ./exports/ --pc-params ./params/DEV.par
              python informatica_to_dbt.py --idmc --token <tok> --pod-url https://usw1.dm-us.informaticacloud.com
              python informatica_to_dbt.py --pc --pc-mode soap --pc-host myhost --pc-repo DEV --pc-user admin --pc-password secret
              python informatica_to_dbt.py --pc --pc-mode db --pc-db-url "oracle+cx_oracle://u:p@h:1521/SID"
              python informatica_to_dbt.py --pc --pc-mode xml --xml ./pc_exports/ --pc-params ./params/DEV.par
        """),
    )
    mg = ap.add_mutually_exclusive_group(required=True)
    mg.add_argument("--xml",  metavar="PATH")
    mg.add_argument("--idmc", action="store_true")
    mg.add_argument("--pc",   action="store_true")

    ap.add_argument("--pod-url");    ap.add_argument("--token")
    ap.add_argument("--project-id"); ap.add_argument("--fetch-only", action="store_true")
    ap.add_argument("--pc-mode",     default="xml", choices=["soap","db","xml"])
    ap.add_argument("--pc-host");    ap.add_argument("--pc-port", default="7333")
    ap.add_argument("--pc-repo");    ap.add_argument("--pc-user")
    ap.add_argument("--pc-password", default=os.environ.get("PC_PASSWORD",""))
    ap.add_argument("--pc-https",    action="store_true")
    ap.add_argument("--pc-db-url");  ap.add_argument("--pc-params", metavar="PATH")
    ap.add_argument("--json",        metavar="PATH")
    ap.add_argument("--out",         default="./dbt_output")
    ap.add_argument("--src-dialect", default="duckdb", choices=DB_DIALECTS)
    ap.add_argument("--tgt-dialect", default="duckdb", choices=DB_DIALECTS)
    args = ap.parse_args()
    out  = Path(args.out)

    taskflows:    list[Taskflow] = []
    mappings:     list[Mapping]  = []
    workflows:    list[Workflow] = []
    connections:  list[dict]     = []
    params:       dict           = {}
    project_meta: Optional[dict] = None

    if args.idmc:
        print("[IDMC] Connecting...")
        try:
            cfg=IDMCConfig.from_args(args); client=IDMCClient(cfg)
            fetcher=IDMCFetcher(client,out)
            mappings,taskflows,connections=fetcher.fetch_all(pid=args.project_id)
        except ValueError as e:
            print(f"Error: {e}"); sys.exit(1)
        if args.fetch_only:
            print(f"Raw files -> {(out/'_idmc_raw').resolve()}"); return

    elif args.pc:
        print(f"[PowerCenter] mode={args.pc_mode}")
        mappings,workflows,connections,params=PCFetcher(args,out).fetch_all()

    else:
        xml_files=collect_files(args.xml,(".xml",))
        print(f"Found {len(xml_files)} XML file(s).\n")
        tfp=InformaticaTaskflowParser(); pmp=PowerMartParser()
        for xf in xml_files:
            print(f"  Parsing: {xf}")
            try:
                tf=tfp.parse_file(xf)
                print(f"    -> Taskflow '{tf.name}' ({len(tf.steps)} step(s))")
                taskflows.append(tf); continue
            except Exception: pass
            try:
                ms,wfs=pmp.parse_file(xf)
                for m in ms:
                    print(f"    -> {'Mapplet' if m.is_mapplet else 'Mapping'} '{m.name}' "
                          f"({len(m.transformations)} transforms, "
                          f"{len(m.connectors)} connectors, "
                          f"{len(m.source_target_pairs)} S->T pair(s))")
                for w in wfs:
                    print(f"    -> Workflow '{w.name}' ({len(w.sessions)} session(s))")
                mappings.extend(ms); workflows.extend(wfs)
            except Exception as e:
                print(f"    Warning: Skipped ({e})")
        if args.json:
            try:
                project_meta=ProjectJsonParser().parse_file(args.json)
                print(f"\n  Project: '{project_meta['name']}'")
            except Exception as e:
                print(f"  Warning: project JSON skipped ({e})")

    if not taskflows and not mappings and not workflows:
        print("\nNothing parsed. Exiting."); sys.exit(1)

    # Build flat params — merge all sections, used for $$variable translation
    flat_params: dict[str, str] = {}
    par_parser = ParameterFileParser()
    for sec_vals in params.values():
        flat_params.update(sec_vals)

    # Also load --pc-params if supplied in --xml or --idmc mode
    if hasattr(args, "pc_params") and args.pc_params and not flat_params:
        try:
            cat = (par_parser.parse_dir(args.pc_params)
                   if Path(args.pc_params).is_dir()
                   else par_parser.parse_file(args.pc_params))
            for sec_vals in cat.values():
                flat_params.update(sec_vals)
            print(f"  Loaded {len(flat_params)} $$variable(s) from {args.pc_params}")
        except Exception as e:
            print(f"  Warning: param load skipped ({e})")

    gen = DbtGenerator(out_dir=str(out), flat_params=flat_params,
                       src_dialect=args.src_dialect, tgt_dialect=args.tgt_dialect)
    gen.generate(taskflows, mappings, workflows, project_meta, connections, params)


if __name__ == "__main__":
    main()
