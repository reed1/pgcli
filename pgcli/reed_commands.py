import re
import subprocess
from pgcli.pgexecute import PGExecute
from pgspecial.main import PGSpecial


class ReedCommands:

    def __init__(self, pgcli):
        self.pgcli = pgcli

    def register_special_commands(self) -> None:
        self.pgcli.pgspecial.register(
            self.drill_one, "\\do", "\\do table [id]", "Get one row from table."
        )
        self.pgcli.pgspecial.register(
            self.drill_down, "\\dd", "\\dd table parent_id", "Drill down a table."
        )
        self.pgcli.pgspecial.register(
            self.drill_up, "\\du", "\\dd table row_id", "Drill up a table."
        )
        self.pgcli.pgspecial.register(
            self.drill_down_kode, "\\dk", "\\dk table kode", "Drill down a table by dot-joined kode."
        )
        self.pgcli.pgspecial.register(
            self.print_tree, "\\tree", "\\tree table root_id", "Print tree of a table."
        )
        self.pgcli.pgspecial.register(
            self.get_columns, "\\gcol", "\\gcol table", "Get columns of a table."
        )
        self.pgcli.pgspecial.register(
            self.get_distinct_count, "\\dc", "\\dc table col1 col2..", "Get distinct column values count."
        )
        self.pgcli.pgspecial.register(
            self.select_schema, "\\ss", "\\ss", "Select and set schema."
        )

    def drill_one(self, pattern, **_):
        if not re.match(r"^\w+( \d+)?$", pattern):
            raise ValueError(
                r"Invalid pattern. Should be \do table [id]")
        [table, *args] = re.split(r'\s+', pattern)
        if len(args) == 0:
            query = f"select * from {table} limit 100"
        elif len(args) == 1:
            row_id = int(args[0])
            query = f"select * from {table} where id = {row_id}"
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def drill_down(self, pattern, **_):
        if not re.match(r"^\w+ \d+$", pattern):
            raise ValueError(
                "Invalid pattern. Should be \\\\dd <table> <parent_id>")
        table, parent_id = pattern.split()
        q_cols = ', '.join(self.find_useful_columns(table))
        query = f"select {q_cols} nama from {
            table} where parent_id = {parent_id}"
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def drill_up(self, pattern, **_):
        if not re.match(r"^\w+ \d+( where .*)?$", pattern):
            raise ValueError(r"Invalid pattern. Should be \du table row_id")
        [table, row_id, *args] = re.split(r'\s+', pattern)
        table, row_id = pattern.split()
        cols = self.find_useful_columns(table)
        q_cols = ', '.join(cols)
        qc_cols = ', '.join([f'c.{x}' for x in cols])
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

    def drill_down_kode(self, pattern, **_):
        if not re.match(r"^\w+ [\w.]+$", pattern):
            raise ValueError(r"Invalid pattern. Should be \dk table kode")
        [table, kode] = re.split(r'\s+', pattern)
        cols = self.find_useful_columns(table)
        kodes = kode.split('.')
        query = f'''
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
        '''
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def print_tree(self, pattern, **_):
        [table, *args] = re.split(r'\s+', pattern)
        if len(args) == 0:
            where = '(parent_id = 0)'
        else:
            root_id = int(args[0])
            where = f'(id = {root_id})'
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
        if not re.match(r"^[\w.]+$", pattern):
            raise ValueError(r"Invalid pattern. Should be \gcol table")
        q_where_schema = '(1=1)'
        table = pattern.strip()
        if '.' in table:
            schema, table = table.split('.')
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
        if not re.match(r"^\w+(\s+\"?\w+\"?)+$", pattern):
            raise ValueError(
                r"Invalid pattern. Should be \dc table [columns]..")
        [table, *columns] = re.split(r'\s+', pattern)
        cols = ', '.join(columns)
        query = f'select {
            cols}, count(*) as cnt from {table} group by {cols} order by {cols}'
        on_error_resume = self.pgcli.on_error == "RESUME"
        return self.pgcli.pgexecute.run(
            query,
            self.pgcli.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.pgcli.explain_mode,
        )

    def select_schema(self, pattern, **_):
        schema_arg = pattern.strip()
        if schema_arg == '':
            query = "SELECT schema_name FROM information_schema.schemata"
            result = self.pgcli.pgexecute.run(query)
            schemas = [row[0]
                       for _, cur, *_ in result for row in cur.fetchall()]
            filtereds = sorted([e for e in schemas
                                if not e.startswith('pg_') and e != 'information_schema'])
            schema = subprocess.run(
                ['rofi', '-dmenu', '-p', 'Select schema:'],
                input='\n'.join(filtereds),
                text=True,
                capture_output=True
            ).stdout.strip()
        else:
            schema = schema_arg
        if schema:
            self.pgcli.pgexecute.run(
                f"SET search_path TO '{schema}'",
            )
            print(f"Search path set to {schema}")
        yield (
            None,
            None,
            None,
            None,
        )

    def find_useful_columns(self, table_name: str):
        useful_cols = ['id', 'parent_id', 'level',
                       'kode', 'code', 'nama', 'name']
        res = self.pgcli.pgexecute.run(
            f"select column_name from information_schema.columns where table_name = '{table_name}'")
        for _, cur, *_ in res:
            rows = cur.fetchall()
        columns = [e[0] for e in rows]
        return [e for e in useful_cols if e in columns]
