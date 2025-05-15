import logging
import os
import subprocess
from prompt_toolkit.key_binding import KeyBindings

_logger = logging.getLogger(__name__)


def add_custom_key_bindings(kb, pgcli):

    @kb.add("c-e")
    def _(event):
        """Edit the current input in external editor."""
        _logger.debug("Detected <C-e> key.")
        buff = event.app.current_buffer
        buff.insert_text("\\e")
        buff.validate_and_handle()

    @kb.add("c-b")
    def _(event):
        """Select active schema"""
        _logger.debug("Detected <C-b> key.")
        query = "SELECT schema_name FROM information_schema.schemata"
        result = pgcli.pgexecute.run(query)
        schemas = [row[0] for _, cur, *_ in result for row in cur.fetchall()]
        filtereds = sorted(
            [
                e
                for e in schemas
                if not e.startswith("pg_") and e != "information_schema"
            ]
        )
        sorteds = custom_sort_schemas(filtereds)
        schema = subprocess.run(
            ["rofi", "-dmenu", "-p", "Select schema"],
            input="\n".join(sorteds),
            text=True,
            capture_output=True,
        ).stdout.strip()
        if not schema:
            return
        persists_last_schema(schema)
        buff = event.app.current_buffer
        buff.text = f"set search_path to '{schema}';"
        buff.validate_and_handle()


def custom_sort_schemas(schemas):
    def custom_sort_key(s):
        """
        Sorts alphabetically (a-z) for letters and numerically (9-0) for numbers,
        So that recent schema year comes first.
        """
        parts = list(s)
        res = []
        for part in parts:
            if part.isdigit():
                # reverse the order of digits
                res.append({str(x): str(9 - x) for x in range(10)}[part])
            else:
                res.append(part)
        return "".join(res)

    return sorted(schemas, key=custom_sort_key)


def persists_last_schema(schema_name: str):
    dbconfig_id = os.environ.get("DBCONFIG_ID")
    fcache = os.path.expanduser(f"~/.cache/rlocal/db/{dbconfig_id}.last_schema")
    with open(fcache, "w") as f:
        f.write(schema_name)
