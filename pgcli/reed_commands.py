import csv
import io
import json
import re
import os
import subprocess

import click

from pgcli.packages.sqlcompletion import Schema, Table, Column
from pgcli.reed_watch import handle_watch_command as reed_handle_watch_command

DB_SOCKET_ENV = "DB_SOCKET"


def _build_and_format_tree(rows):
    """Build tree structure from rows and format with box-drawing characters."""
    if not rows:
        return rows

    # Build the tree structure: {parent_level: [(level, count), ...]}
    children_map = {}
    depth_map = {}  # {(parent_level, level): depth}

    for depth, parent_level, level, cnt in rows:
        key = parent_level if parent_level else None
        if key not in children_map:
            children_map[key] = []
        children_map[key].append((level, cnt))
        depth_map[(parent_level, level)] = depth

    # Traverse and format the tree
    result = []

    def traverse(parent_level, prefix_parts):
        """Recursively traverse and format tree nodes."""
        children = children_map.get(parent_level, [])

        for i, (level, cnt) in enumerate(children):
            is_last = i == len(children) - 1
            depth = depth_map[(parent_level, level)]

            # Build the prefix for this node
            if depth == 0:
                prefix = ""
            else:
                # Add branch for current node
                if is_last:
                    prefix = "".join(prefix_parts) + "└─ "
                else:
                    prefix = "".join(prefix_parts) + "├─ "

            formatted_level = prefix + level
            result.append((depth, formatted_level, cnt))

            # Recursively traverse children
            if level in children_map:
                # Prepare prefix for children
                if depth == 0:
                    new_prefix_parts = []
                else:
                    if is_last:
                        new_prefix_parts = prefix_parts + ["   "]
                    else:
                        new_prefix_parts = prefix_parts + ["│  "]
                traverse(level, new_prefix_parts)

    # Start traversal from root (parent_level = None)
    traverse(None, [])

    return result


# Track the last tabular command context
_last_tabular_command_table = None
# Last schema seen on a qualified table; preserved across unqualified references
# so "open table" can reuse it (select * from <schema>.<table> ...).
_last_schema = None

# One socket server per pgcli process, started lazily when first needed.
_socket_server = None


def reed_tabular_command(func):
    """Track the table a tabular command was last invoked on, for drill context."""

    def wrapper(*args, **kwargs):
        global _last_tabular_command_table, _last_schema

        pattern = kwargs.get("pattern")
        if not pattern and len(args) > 1:
            pattern = args[1]

        if pattern:
            arg_parts = re.split(r"\s+", pattern.strip())
            if arg_parts and arg_parts[0]:
                _last_tabular_command_table = arg_parts[0]
                if "." in arg_parts[0]:
                    _last_schema = arg_parts[0].split(".", 1)[0]

        return func(*args, **kwargs)

    return wrapper


def set_active_table_from_sql(sql):
    """Record the first table referenced by a plain SQL query as the drill context.

    Lets `select * from <table>` feed drill up/down just like the \\do family.
    Reuses pgcli's own table extractor; backslash commands yield no tables and
    leave the context untouched.
    """
    global _last_tabular_command_table, _last_schema

    from pgcli.packages.parseutils.tables import extract_tables

    tables = extract_tables(sql)
    if not tables:
        return
    ref = tables[0]
    if ref.schema:
        _last_schema = ref.schema
    _last_tabular_command_table = f"{ref.schema}.{ref.name}" if ref.schema else ref.name


def _results_to_csv(results):
    out = io.StringIO()
    writer = csv.writer(out)
    for _title, cur, headers, _status, _sql, _success, _is_special in results:
        if headers:
            writer.writerow(headers)
        if cur is not None:
            for row in cur:
                writer.writerow(["" if value is None else value for value in row])
    return out.getvalue()


def ensure_socket_server(reed_commands):
    """Start the per-process socket server on first use and export its path.

    VisiData runs as pgcli's pager, so while it is open pgcli's main thread is
    blocked in echo_via_pager and never touches the connection — the server can
    safely reuse the live connection to answer drill requests.
    """
    global _socket_server
    if _socket_server is not None:
        return _socket_server

    from pgcli.packages.socket_server import SocketServer

    server = SocketServer(lambda request: _handle_request(reed_commands, request))
    server.start()
    os.environ[DB_SOCKET_ENV] = server.path
    _socket_server = server
    return server


def shutdown_socket_server():
    global _socket_server
    if _socket_server is not None:
        _socket_server.stop()
        _socket_server = None
    os.environ.pop(DB_SOCKET_ENV, None)


def _handle_request(reed_commands, request):
    req = json.loads(request)
    try:
        csv_text = _run_action(reed_commands, req)
        return json.dumps({"ok": True}).encode() + b"\n" + csv_text.encode()
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}).encode() + b"\n"


def _run_action(reed_commands, req):
    action = req["action"]

    if action in ("drill_up", "drill_down"):
        table = _last_tabular_command_table
        if not table:
            raise RuntimeError("No active table context to drill from")
        pattern = f"{table} {req['id']}"
        results = reed_commands.drill_up(pattern) if action == "drill_up" else reed_commands.drill_down(pattern)
    elif action == "open_table":
        table = req["table"]
        if _last_schema and "." not in table:
            table = f"{_last_schema}.{table}"
        results = reed_commands.drill_one(f"{table} {req['id']}")
    else:
        raise RuntimeError(f"Unknown action: {action}")

    return _results_to_csv(results)


class ReedCommands:
    TABLE_PATTERN = r'[\w_."]+'

    def __init__(self, pgcli):
        self.pgcli = pgcli

    def register_special_commands(self) -> None:
        # Patch watch command to use ASCII format and clear screen
        self.pgcli.handle_watch_command = lambda text: reed_handle_watch_command(self.pgcli, text)

        self.pgcli.pgspecial.register(
            self.drill_one,
            "\\do",
            "\\do table [id|order by ...]",
            "Get rows from table with optional id or order clause.",
        )
        self.pgcli.pgspecial.register(self.drill_down, "\\dd", "\\dd table parent_id", "Drill down a table.")
        self.pgcli.pgspecial.register(
            self.drill_down_recursive,
            "\\ddr",
            "\\ddr table row_id [where ...]",
            "Drill down recursive.",
        )
        self.pgcli.pgspecial.register(self.drill_up, "\\du", "\\dd table row_id", "Drill up a table.")
        self.pgcli.pgspecial.register(
            self.drill_down_kode,
            "\\dk",
            "\\dk table kode",
            "Drill down a table by dot-joined kode.",
        )
        self.pgcli.pgspecial.register(self.print_tree, "\\tree", "\\tree table root_id", "Print tree of a table.")
        self.pgcli.pgspecial.register(self.get_columns, "\\gcol", "\\gcol table", "Get columns of a table.")
        self.pgcli.pgspecial.register(
            self.get_distinct_count,
            "\\dc",
            "\\dc table col1 col2..",
            "Get distinct column values count.",
        )
        self.pgcli.pgspecial.register(self.show_create_table, "\\sct", "\\sct table", "Show create table.")
        self.pgcli.pgspecial.register(
            self.show_create_table_dump,
            "\\sctd",
            "\\sctd table",
            "Show create table (using pg_dump).",
        )
        self.pgcli.pgspecial.register(
            self.load_table,
            "\\lt",
            "\\lt '<path>' <table>",
            "Load data from file into table.",
        )
        self.pgcli.pgspecial.register(
            self.truncate_table,
            "\\tc",
            "\\tc [table...]",
            "Truncate table with restart identity.",
        )
        self.pgcli.pgspecial.register(
            self.directed_format,
            "\\df",
            "\\df [recipe]",
            "Directed format - set pager and table format",
        )
        self.pgcli.pgspecial.register(
            self.table_row_count,
            "\\trc",
            "\\trc",
            "Show tables with row counts (pg_stat + max id).",
        )
        self.pgcli.pgspecial.register(
            self.info_tables,
            "\\it",
            "\\it [pattern]",
            "Search tables by name pattern",
        )
        self.pgcli.pgspecial.register(
            self.info_columns,
            "\\ic",
            "\\ic [pattern]",
            "Search columns by name pattern",
        )

    @reed_tabular_command
    def drill_one(self, pattern, **_):
        pattern = pattern.strip()
        [table, *args] = re.split(r"\s+", pattern)

        # Validate table name
        if not re.match(rf"^{self.TABLE_PATTERN}$", table):
            raise ValueError(r"Invalid table name")

        if len(args) == 0:
            # \do table
            query = f"select * from {table} limit 100"
        elif len(args) == 1 and args[0].isdigit():
            # \do table 123
            row_id = int(args[0])
            query = f"select * from {table} where id = {row_id}"
        else:
            # \do table order by id [limit 50]
            rest_of_query = " ".join(args)

            # Check if there's already a limit clause
            if "limit" in rest_of_query.lower():
                query = f"select * from {table} {rest_of_query}"
            else:
                query = f"select * from {table} {rest_of_query} limit 100"

        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    @reed_tabular_command
    def drill_down(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN} \d+$", pattern):
            raise ValueError("Invalid pattern. Should be \\\\dd <table> <parent_id>")
        table, parent_id = pattern.split()
        q_cols = ", ".join(self.get_filtered_columns(table))
        query = f"select {q_cols} from {table} where parent_id = {parent_id}"
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    @reed_tabular_command
    def drill_up(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN} \d+( where .*)?$", pattern):
            raise ValueError(r"Invalid pattern. Should be \du table row_id")
        [table, row_id, *args] = re.split(r"\s+", pattern)
        table, row_id = pattern.split()
        cols = self.get_filtered_columns(table)
        q_cols = ", ".join(cols)
        qc_cols = ", ".join([f"c.{x}" for x in cols])
        query = f"""
        with recursive cte as (
            select {q_cols}, 1 as depth from {table} where id = {row_id}
            union all
            select {qc_cols}, cte.depth + 1 from {table} as c
            inner join cte on c.id = cte.parent_id
        )
        select {q_cols} from cte {' '.join(args)} order by depth desc
        """
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    @reed_tabular_command
    def drill_down_recursive(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN} \d+( where .*)?$", pattern):
            raise ValueError(r"Invalid pattern. Should be \ddr table row_id [where ...]")
        [table, row_id, *args] = re.split(r"\s+", pattern)
        cols = self.get_filtered_columns(table)
        extra = " ".join(args)
        q_where = "(1=1)"
        if extra.startswith("where "):
            q_where = extra[6:]
        q_cols = ", ".join(cols)
        qc_cols = ", ".join([f"c.{col}" for col in cols])
        query = f"""
        with recursive cte as (
            select {q_cols}, 0 as depth from {table} where id = {row_id}
            union all
            select {qc_cols}, cte.depth + 1
            from {table} as c
            inner join cte on c.parent_id = cte.id
            where {q_where}
        )
        select depth, {q_cols} from cte order by depth, id
        """
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    @reed_tabular_command
    def drill_down_kode(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN} [\w.]+$", pattern):
            raise ValueError(r"Invalid pattern. Should be \dk table kode")
        [table, kode] = re.split(r"\s+", pattern)
        cols = self.get_filtered_columns(table)
        kodes = kode.split(".")
        query = f"""
        with recursive td as (
            {' union all '.join([
            f"select {i} as depth, '{k}' as kode"
            for i, k in enumerate(kodes)
        ])}
        ),
        t as (
            select {', '.join(cols)}, 0 as depth, kode as kode_full
            from {table}
            where
                parent_id = 0 and
                kode = (select kode from td where depth = 0)
            union all
            select c.{', c.'.join(cols)}, t.depth + 1 as depth, concat(t.kode_full, '.', c.kode) as kode_full
            from t
            inner join {table} as c on
                c.parent_id = t.id and
                c.kode = (select kode from td where depth = t.depth + 1)
        )
        select kode_full, {', '.join(cols)} from t
        order by depth, id
        """
        with open("/tmp/dk_query.sql", "w") as f:
            f.write(query)
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def print_tree(self, pattern, **_):
        [table, *args] = re.split(r"\s+", pattern)
        if len(args) == 0:
            where = "(parent_id = 0)"
        else:
            root_id = int(args[0])
            where = f"(id = {root_id})"
        query = f"""
        with recursive cte as (
            select
            id,
            level,
            cast(NULL as text) as parent_level,
            0 as depth
            from {table}
            where {where}
            union all
            select
            t.id,
            t.level,
            cte.level as parent_level,
            cte.depth + 1
            from {table} t
            inner join cte on t.parent_id = cte.id
        )
        select
            depth,
            parent_level,
            level,
            count(*) as cnt
        from cte
        group by depth, parent_level, level
        """
        on_error_resume = self.pgcli.on_error == "RESUME"
        results = self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )
        for title, cur, headers, status, sql, success, is_special in results:
            if cur:
                rows = list(cur)
                formatted_rows = _build_and_format_tree(rows)
                filtered_headers = ["depth", "level", "cnt"]
                yield title, formatted_rows, filtered_headers, status, sql, success, is_special
            else:
                yield title, cur, headers, status, sql, success, is_special

    def get_columns(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN}$", pattern):
            raise ValueError(r"Invalid pattern. Should be \gcol table")
        q_where_schema = "(1=1)"
        table = pattern.strip()
        if "." in table:
            schema, table = table.split(".")
            q_where_schema = f"table_schema = '{schema}'"

        query = f"""
        select
            column_name as column,
            data_type as type
        from information_schema.columns
        where table_name = '{table}' and {q_where_schema}
        order by ordinal_position
        """
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def get_distinct_count(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN}(\s+{self.TABLE_PATTERN})+$", pattern):
            raise ValueError(r"Invalid pattern. Should be \dc table [columns]..")
        [table, *columns] = re.split(r"\s+", pattern)
        cols = ", ".join(columns)
        query = f"select {cols}, count(*) as cnt from {table} group by {cols} order by {cols}"
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def show_create_table(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN}$", pattern):
            raise ValueError(r"Invalid pattern. Should be \sct table")
        table = pattern.strip()

        if "." in table:
            schema, table_name = table.split(".", 1)
            schema_filter_col = f"'{schema}'"
            schema_filter_idx = f"'{schema}'"
        else:
            table_name = table
            schema_filter_col = "current_schema()"
            schema_filter_idx = "current_schema()"

        query = f"""
        SELECT 'column' as type,
               c.column_name as name,
               c.data_type ||
               CASE
                   WHEN c.character_maximum_length IS NOT NULL
                   THEN '(' || c.character_maximum_length || ')'
                   WHEN c.data_type IN ('numeric', 'decimal') AND c.numeric_precision IS NOT NULL
                   THEN '(' || c.numeric_precision ||
                        CASE WHEN c.numeric_scale IS NOT NULL
                        THEN ',' || c.numeric_scale ELSE '' END || ')'
                   ELSE ''
               END ||
               CASE WHEN c.is_nullable = 'NO' THEN ' not null' ELSE '' END ||
               CASE WHEN c.column_default IS NOT NULL
                    THEN ' default ' || c.column_default ELSE '' END
               as definition,
               c.ordinal_position as sort_order
        FROM information_schema.columns c
        WHERE c.table_name = '{table_name}'
          AND c.table_schema = {schema_filter_col}
        UNION ALL
        SELECT 'index' as type,
               i.indexname as name,
               i.indexdef as definition,
               0 as sort_order
        FROM pg_indexes i
        WHERE i.tablename = '{table_name}'
          AND i.schemaname = {schema_filter_idx}
        ORDER BY sort_order, name
        """

        on_error_resume = self.pgcli.on_error == "RESUME"
        result = self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

        # Extract rows from result
        rows = None
        for _, cur, *_ in result:
            if hasattr(cur, "fetchall"):
                rows = cur.fetchall()
            break

        if not rows:
            click.secho(f"No data returned for table {table}", fg="yellow")
            return [(None, [], [], None, "", True, False)]

        # Parse the combined result
        create_sql = self._parse_combined_output(table, rows)

        with open("/tmp/sct_query.sql", "w") as f:
            f.write(create_sql)
        subprocess.run(
            [
                "kitty",
                "@",
                "launch",
                "--type=overlay",
                "show-sql",
                "-l",
                "postgresql",
                "/tmp/sct_query.sql",
            ],
            check=True,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        return [(None, [], [], None, "", True, False)]

    def _parse_combined_output(self, table, rows):
        """Parse combined column and index output to generate CREATE TABLE statement."""
        column_lines = []
        index_lines = []

        for row in rows:
            row_type = row[0]
            name = row[1]
            definition = row[2]

            if row_type == "column":
                column_lines.append(f"  {name} {definition}")
            elif row_type == "index":
                index_lines.append(f"{definition};")

        # Build CREATE TABLE statement
        create_table = "-- WARNING: approximate fast lookup, not runnable DDL. Use \\sctd for the real pg_dump.\n"
        create_table += f"CREATE TABLE {table} (\n"
        create_table += ",\n".join(column_lines)
        create_table += "\n);"

        # Add index statements
        if index_lines:
            create_table += "\n\n"
            create_table += "\n".join(index_lines)

        return create_table

    def show_create_table_dump(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN}$", pattern):
            raise ValueError(r"Invalid pattern. Should be \sctd table")
        table = pattern.strip()
        pge = self.pgcli.pgexecute
        output = subprocess.run(
            [
                "pg_dump",
                "-U",
                pge.user,
                "-d",
                pge.dbname,
                "-t",
                table,
                "-h",
                pge.host,
                *(["-p", pge.port] if pge.port else []),
                "--schema-only",
                "--no-comments",
                "--no-owner",
                "--no-acl",
            ],
            env={"PGPASSWORD": pge.password, **os.environ},
            text=True,
            capture_output=True,
            check=True,
        )

        def extract_table_dump(dump: str):
            # Keep every DDL statement, dropping only pg_dump's session boilerplate and
            # its psql meta-commands, which sql-formatter refuses to parse.
            noise = re.compile(r"^(--|\\|SET\s|SELECT pg_catalog\.set_config)")
            kept = "\n".join(line for line in dump.splitlines() if not noise.match(line))
            ddl = re.sub(r"\n{3,}", "\n\n", kept).strip()

            if not ddl:
                raise ValueError("No DDL found in the dump.")

            return ddl

        table_dump = extract_table_dump(output.stdout).strip()
        with open("/tmp/sct_query.sql", "w") as f:
            f.write(table_dump)
        subprocess.run(
            [
                "kitty",
                "@",
                "launch",
                "--type=overlay",
                "show-sql",
                "-l",
                "postgresql",
                "/tmp/sct_query.sql",
            ],
            check=True,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        return [(None, [], [], None, "", True, False)]

    def _get_current_schema(self):
        res = self.pgcli.pgexecute.run("SELECT current_schema()")
        for _, cur, *_ in res:
            rows = cur.fetchall()
            if rows:
                return rows[0][0]
        return "public"

    def get_filtered_columns(self, table_name: str):
        schema = self._get_current_schema()
        res = self.pgcli.pgexecute.run(
            f"select column_name from information_schema.columns where table_name = '{table_name}' and table_schema = '{schema}' order by ordinal_position"
        )
        for _, cur, *_ in res:
            rows = cur.fetchall()
        columns = [e[0] for e in rows]

        excluded = {"depth", "kode_full"}
        if os.environ.get("USE_MINIMAL_COLUMN_SET", "0") == "1":
            minimal_column_set = [
                "id",
                "parent_id",
                "level",
                "kode",
                "code",
                "nama",
                "name",
            ]
            return [e for e in minimal_column_set if e in columns and e not in excluded]
        else:
            return [e for e in columns if e not in excluded]

    def load_table(self, pattern, **_):
        # Match file path in quotes and table name
        match = re.match(r"^'([^']+)'\s+(\w+)$", pattern.strip())
        if not match:
            raise ValueError(r"Invalid pattern. Should be \\lt '<path>' <table>")

        file_path = match.group(1)
        table = match.group(2)

        # PostgreSQL COPY command syntax
        query = f"\\copy {table} from '{file_path}' with (format csv, header true, delimiter ',')"

        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def truncate_table(self, pattern, **_):
        if not re.match(rf"^{self.TABLE_PATTERN}(\s+{self.TABLE_PATTERN})*$", pattern):
            raise ValueError(r"Invalid pattern. Should be \tc table [table...]")
        tables = re.split(r"\s+", pattern.strip())

        on_error_resume = self.pgcli.on_error == "RESUME"
        for table in tables:
            query = f"truncate table {table} restart identity"
            self.pgcli.pgexecute.run(
                query,
                self.pgcli.pgspecial,
                on_error_resume=on_error_resume,
                explain_mode=self.pgcli.explain_mode,
            )

        return [(None, [], [], None, f"Truncated {len(tables)} table(s) successfully", True, False)]

    def directed_format(self, pattern, **_):
        arg = pattern.strip().upper() if pattern else "A"

        if arg == "A":
            # Recipe A: visidata-db pager with CSV format
            self.pgcli.pgspecial.pset_pager("always")
            return self.pgcli.pgexecute.run("\\T csv", self.pgcli.pgspecial)
        elif arg == "C":
            # Recipe C: no pager with ASCII format
            self.pgcli.pgspecial.pset_pager("off")
            return self.pgcli.pgexecute.run("\\T ascii", self.pgcli.pgspecial)
        else:
            raise ValueError(f"Unknown recipe '{arg}'. Use A or C.")

    def table_row_count(self, pattern, **_):
        # Step 1: Get tables and check for id column
        schema_query = """
        SELECT t.table_name,
               MAX(CASE WHEN c.column_name = 'id'
                   AND c.data_type IN ('integer', 'bigint', 'smallint') THEN 1 ELSE 0 END) as has_id
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c
            ON t.table_name = c.table_name
            AND t.table_schema = c.table_schema
            AND c.column_name = 'id'
        WHERE t.table_schema = current_schema()
            AND t.table_type = 'BASE TABLE'
        GROUP BY t.table_name
        ORDER BY t.table_name
        """

        results = self.pgcli.pgexecute.run(schema_query, self.pgcli.pgspecial)
        tables = []
        for _, cur, *_ in results:
            if cur:
                tables = list(cur)

        if not tables:
            yield (None, [], [], None, "", True, False)
            return

        # Step 2: Build UNION ALL for max(id) - very fast, no count(*)
        tables_with_id = [t[0] for t in tables if t[1] == 1]

        max_id_map = {}
        if tables_with_id:
            union_parts = [f"SELECT '{t}' as table_name, MAX(id)::text as max_id FROM {t}" for t in tables_with_id]
            max_id_query = " UNION ALL ".join(union_parts)
            results = self.pgcli.pgexecute.run(max_id_query, self.pgcli.pgspecial)
            for _, cur, *_ in results:
                if cur:
                    for row in cur:
                        max_id_map[row[0]] = row[1]

        # Step 3: Get pg_stat estimated counts
        stat_query = """
        SELECT relname, n_live_tup
        FROM pg_stat_user_tables
        WHERE schemaname = current_schema()
        """
        results = self.pgcli.pgexecute.run(stat_query, self.pgcli.pgspecial)
        stat_map = {}
        for _, cur, *_ in results:
            if cur:
                for row in cur:
                    stat_map[row[0]] = row[1]

        # Step 4: Combine results
        combined_rows = []
        for table_name, has_id in tables:
            pg_stat = stat_map.get(table_name, 0)
            max_id = max_id_map.get(table_name)
            combined_rows.append((table_name, pg_stat, max_id))

        headers = ["table_name", "pg_stat_count", "max_id"]
        status = f"SELECT {len(combined_rows)}"
        yield (None, combined_rows, headers, status, "", True, False)

    def info_tables(self, pattern, **_):
        pattern = pattern.strip() if pattern else "%"
        if pattern.isalnum():
            pattern = f"%{pattern}%"
        else:
            pattern = pattern.replace("*", "%")
        query = f"select * from information_schema.tables where table_name like '{pattern}'"
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def info_columns(self, pattern, **_):
        pattern = pattern.strip() if pattern else "%"
        if pattern.isalnum():
            pattern = f"%{pattern}%"
        else:
            pattern = pattern.replace("*", "%")
        query = f"select * from information_schema.columns where column_name like '{pattern}'"
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )


def is_reed_command(cmd):
    return cmd in (
        "\\do",
        "\\dd",
        "\\ddr",
        "\\du",
        "\\dk",
        "\\tree",
        "\\it",
        "\\ic",
        "\\gcol",
        "\\dc",
        "\\sct",
        "\\sctd",
        "\\lt",
        "\\tc",
        "\\df",
        "\\trc",
    )


def reed_suggestions(cmd, arg):
    if not arg or not arg.strip():
        # No argument yet, suggest tables for most commands
        if cmd == "\\df":
            # For directed format, suggest recipe options - but return nothing since they're just letters
            return ()
        else:
            return (Schema(), Table(schema=None))
    elif cmd in ("\\lt", "\\tc"):
        return (Table(schema=None),)
    else:
        # Check if we're still on the first argument (table name)
        args = arg.split()
        if len(args) == 1 and not arg.endswith(" "):
            # Still typing the table name
            if "." in args[0]:
                # Schema-qualified table
                schema = args[0].split(".")[0]
                return (Table(schema=schema),)
            else:
                return (Schema(), Table(schema=None))
        # For \dc command that needs column names after the table
        elif cmd == "\\dc":
            if len(args) >= 1 and (arg.endswith(" ") or len(args) > 1):
                # Already have table name, suggest columns for grouping
                from pgcli.packages.parseutils.tables import TableReference

                table_name = args[0]
                if "." in table_name:
                    schema, table = table_name.split(".", 1)
                    table_ref = TableReference(schema, table, None, False)
                else:
                    table_ref = TableReference(None, table_name, None, False)
                return (Column(table_refs=(table_ref,), qualifiable=False),)
    return []
