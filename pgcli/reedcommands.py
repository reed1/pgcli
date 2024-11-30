import re
from pgcli.main import PGCli
from pgcli.pgexecute import PGExecute


def register(pgcli: PGCli) -> None:
    (ReedCommands(pgcli)).register_special_commands()


class ReedCommands:
    pgcli: PGCli

    def __init__(self, pgcli: PGCli):
        self.pgcli = pgcli
        self.pgspecial = pgcli.pgspecial
        self.pgexecute = pgcli.pgexecute
        self.on_error = pgcli.on_error
        self.explain_mode = pgcli.explain_mode
        self.register_special_commands()

    def register_special_commands(self) -> None:
        self.pgspecial.register(
            self.drill_one, "\\do", "\\do table [id]", "Get one row from table."
        )
        self.pgspecial.register(
            self.drill_down, "\\dd", "\\dd table parent_id", "Drill down a table."
        )
        self.pgspecial.register(
            self.drill_up, "\\du", "\\dd table row_id", "Drill up a table."
        )
        self.pgspecial.register(
            self.drill_down_kode, "\\dk", "\\dk table kode", "Drill down a table by dot-joined kode."
        )
        self.pgspecial.register(
            self.print_tree, "\\tree", "\\tree table root_id", "Print tree of a table."
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
        on_error_resume = self.on_error == "RESUME"
        return self.pgexecute.run(
            query,
            self.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.explain_mode,
        )

    def drill_down(self, pattern, **_):
        if not re.match(r"^\w+ \d+$", pattern):
            raise ValueError(
                "Invalid pattern. Should be \\\\dd <table> <parent_id>")
        table, parent_id = pattern.split()
        q_cols = ', '.join(self.find_useful_columns(table, self.pgexecute))
        query = f"select {q_cols} nama from {
            table} where parent_id = {parent_id}"
        self.find_useful_columns(table, self.pgexecute)
        on_error_resume = self.on_error == "RESUME"
        return self.pgexecute.run(
            query,
            self.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.explain_mode,
        )

    def drill_up(self, pattern, **_):
        if not re.match(r"^\w+ \d+( where .*)?$", pattern):
            raise ValueError(r"Invalid pattern. Should be \du table row_id")
        [table, row_id, *args] = re.split(r'\s+', pattern)
        table, row_id = pattern.split()
        cols = self.find_useful_columns(table, self.pgexecute)
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
        on_error_resume = self.on_error == "RESUME"
        return self.pgexecute.run(
            query,
            self.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.explain_mode,
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
        on_error_resume = self.on_error == "RESUME"
        return self.pgexecute.run(
            query,
            self.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.explain_mode,
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
        on_error_resume = self.on_error == "RESUME"
        return self.pgexecute.run(
            query,
            self.pgspecial,
            on_error_resume=on_error_resume,
            explain_mode=self.explain_mode,
        )

    def find_useful_columns(self, table_name: str):
        useful_cols = ['id', 'parent_id', 'level',
                       'kode', 'code', 'nama', 'name']
        res = self.pgexecute.run(
            f"select column_name from information_schema.columns where table_name = '{table_name}'")
        for _, cur, *_ in res:
            rows = cur.fetchall()
        columns = [e[0] for e in rows]
        return [e for e in useful_cols if e in columns]
