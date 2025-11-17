import re
import os
import subprocess

from pgcli.packages.sqlcompletion import Schema, Table, Column

RVISIDATA_DB_LAST_REPLY_FILE = "/tmp/rlocal/visidata/last-reply"

# Track the last tabular command context
_last_tabular_command_table = None


def reed_tabular_command(func):
    """Decorator to mark commands as reed tabular commands."""

    # Wrap the function to track when it's called
    def wrapper(*args, **kwargs):
        global _last_tabular_command_table

        # Clean up the reply file
        if os.path.exists(RVISIDATA_DB_LAST_REPLY_FILE):
            os.remove(RVISIDATA_DB_LAST_REPLY_FILE)

        # Extract table and id from arguments for tracking
        pattern = None
        if kwargs.get("pattern"):
            pattern = kwargs["pattern"]
        elif len(args) > 1:
            pattern = args[1]

        if pattern:
            arg_parts = re.split(r"\s+", pattern.strip())
            if len(arg_parts) >= 1:
                _last_tabular_command_table = arg_parts[0]

        return func(*args, **kwargs)

    return wrapper


def on_pager_close():
    """Called after the pager closes. Returns pending command if there is one."""
    global _last_tabular_command_table

    if not os.path.exists(RVISIDATA_DB_LAST_REPLY_FILE):
        return None

    with open(RVISIDATA_DB_LAST_REPLY_FILE, "r") as f:
        last_reply = f.read().strip()

    if not last_reply or not _last_tabular_command_table:
        return None

    # Parse the reply format
    if last_reply.startswith("drill_up."):
        # Extract ID from drill_up.<id> format
        parts = last_reply.split(".")
        new_id = parts[1]
        command_to_execute = f"\\du {_last_tabular_command_table} {new_id}"
        return command_to_execute
    elif last_reply.startswith("drill_down."):
        # Extract ID from drill_down.<id> format
        parts = last_reply.split(".")
        new_id = parts[1]
        command_to_execute = f"\\dd {_last_tabular_command_table} {new_id}"
        return command_to_execute

    return None


class ReedCommands:
    TABLE_PATTERN = r'[\w_."]+'

    def __init__(self, pgcli):
        self.pgcli = pgcli

    def register_special_commands(self) -> None:
        self.pgcli.pgspecial.register(
            self.drill_one,
            "\\do",
            "\\do table [id|order by ...]",
            "Get rows from table with optional id or order clause.",
        )
        self.pgcli.pgspecial.register(
            self.drill_down, "\\dd", "\\dd table parent_id", "Drill down a table."
        )
        self.pgcli.pgspecial.register(
            self.drill_down_recursive, "\\ddr", "\\ddr table row_id [where ...]", "Drill down recursive."
        )
        self.pgcli.pgspecial.register(
            self.drill_up, "\\du", "\\dd table row_id", "Drill up a table."
        )
        self.pgcli.pgspecial.register(
            self.drill_down_kode,
            "\\dk",
            "\\dk table kode",
            "Drill down a table by dot-joined kode.",
        )
        self.pgcli.pgspecial.register(
            self.print_tree, "\\tree", "\\tree table root_id", "Print tree of a table."
        )
        self.pgcli.pgspecial.register(
            self.get_columns, "\\gcol", "\\gcol table", "Get columns of a table."
        )
        self.pgcli.pgspecial.register(
            self.get_distinct_count,
            "\\dc",
            "\\dc table col1 col2..",
            "Get distinct column values count.",
        )
        self.pgcli.pgspecial.register(
            self.show_create_table, "\\sct", "\\sct table", "Show create table."
        )
        self.pgcli.pgspecial.register(
            self.load_table,
            "\\lt",
            "\\lt '<path>' <table>",
            "Load data from file into table.",
        )
        self.pgcli.pgspecial.register(
            self.directed_format,
            "\\df",
            "\\df [recipe]",
            "Directed format - set pager and table format",
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
            select id, parent_id,
            cast(level as text) as level_full,
            level, 0 as depth
            from {table}
            where {where}
            union all
            select t.id, t.parent_id,
            concat(cte.level_full, '-', t.level) as level_full,
            t.level, cte.depth + 1
            from {table} t
            inner join cte on t.parent_id = cte.id
        )
        select
            depth,
            concat(
                repeat('*', depth),
                case when depth > 0 then ' ' else '' end,
                level) as level,
            count(*) as cnt
        from cte
        group by depth, level
        order by min(level_full)
        """
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

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
            # Split by double newlines to get blocks
            blocks = dump.split('\n\n')

            # Filter blocks that start with CREATE (after stripping)
            create_blocks = [
                block.strip()
                for block in blocks
                if block.strip().startswith('CREATE')
            ]

            if not create_blocks:
                raise ValueError("No CREATE statements found in the dump.")

            # Join blocks back with double newline
            return '\n\n'.join(create_blocks)

        table_dump = extract_table_dump(output.stdout).strip()
        with open("/tmp/sct_query.sql", "w") as f:
            f.write(table_dump)
        subprocess.run(
            ["kitty", "@", "launch", "--type=overlay", "show-sql", "/tmp/sct_query.sql"],
            check=True,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        return [(None, [], [], None, "", True, False)]

    def get_filtered_columns(self, table_name: str):
        res = self.pgcli.pgexecute.run(
            f"select column_name from information_schema.columns where table_name = '{table_name}'"
        )
        for _, cur, *_ in res:
            rows = cur.fetchall()
        columns = [e[0] for e in rows]

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
            return [e for e in minimal_column_set if e in columns]
        else:
            return columns

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


def is_reed_command(cmd):
    return cmd in (
        "\\do",
        "\\dd",
        "\\ddr",
        "\\du",
        "\\dk",
        "\\tree",
        "\\gcol",
        "\\dc",
        "\\sct",
        "\\lt",
        "\\df",
    )


def reed_suggestions(cmd, arg):
    if not arg or not arg.strip():
        # No argument yet, suggest tables
        return (Schema(), Table(schema=None))
    elif cmd == "\\lt":
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
