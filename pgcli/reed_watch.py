import datetime as dt
from time import sleep, time

import click
import pgspecial as special


def handle_watch_command(pgcli, text):
    watch_command, timing = special.get_watch_command(text)

    if watch_command is not None and not watch_command.strip():
        try:
            watch_command = pgcli.query_history[-1].query
        except IndexError:
            click.secho("\\watch cannot be used with an empty query", err=True, fg="red")
            return

    if watch_command:
        pgcli.watch_command = watch_command
        try:
            _run_watch_loop(pgcli, watch_command, timing)
        except KeyboardInterrupt:
            pass
        pgcli.watch_command = None
    else:
        query = pgcli.execute_command(text)
        pgcli.query_history.append(query)


def _run_watch_loop(pgcli, watch_command, timing):
    from pgcli.main import format_output, OutputSettings

    last_data = None

    while True:
        start = time()
        res = pgcli.pgexecute.run(
            watch_command,
            pgcli.pgspecial,
            exception_formatter=None,
            on_error_resume=False,
        )

        results = []
        current_data = []
        for title, cur, headers, status, _, success, _ in res:
            rows = list(cur) if cur else None
            results.append((title, rows, headers, status, success))
            current_data.append((rows, headers))

        execution_time = time() - start
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_line = click.style(f"Last run: {timestamp}", fg="bright_black")

        if current_data == last_data:
            click.echo(f"\033[F\033[2K{status_line}")
        else:
            click.clear()
            settings = OutputSettings(
                table_format="ascii",
                dcmlfmt=pgcli.decimal_format,
                floatfmt=pgcli.float_format,
                column_date_formats=pgcli.column_date_formats,
                missingval=pgcli.null_string,
                expanded=False,
                max_width=None,
                case_function=lambda x: x,
                style_output=pgcli.style_output,
                max_field_width=pgcli.max_field_width,
            )
            for title, rows, headers, status, _ in results:
                formatted = format_output(title, rows, headers, status, settings)
                click.echo("\n".join(formatted))
            click.echo(f"Time: {execution_time:.3f}s")
            click.echo(status_line)
            last_data = current_data

        sleep(timing)
