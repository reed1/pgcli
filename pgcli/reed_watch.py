import datetime as dt
from time import sleep

import click
import pgspecial as special


def handle_watch_command(pgcli, text):
    """Custom watch command handler with clear screen and ASCII format."""
    watch_command, timing = special.get_watch_command(text)

    # If we run \watch without a command, apply it to the last query run.
    if watch_command is not None and not watch_command.strip():
        try:
            watch_command = pgcli.query_history[-1].query
        except IndexError:
            click.secho("\\watch cannot be used with an empty query", err=True, fg="red")
            return

    if watch_command:
        original_format = pgcli.table_format
        pgcli.table_format = "ascii"
        pgcli.watch_command = watch_command
        query = None

        try:
            while True:
                click.clear()
                query = pgcli.execute_command(watch_command)
                timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                click.secho(f"Last run: {timestamp}", fg="bright_black")
                sleep(timing)
        except KeyboardInterrupt:
            pass
        finally:
            pgcli.table_format = original_format

        pgcli.watch_command = None
    else:
        query = pgcli.execute_command(text)

    pgcli.query_history.append(query)
