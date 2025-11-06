# pgcli Fork

This is a customized fork of [pgcli](https://github.com/dbcli/pgcli) with enhanced features for database exploration, hierarchical data navigation, and improved workflow integration.

## Hierarchical Data Navigation

Commands for working with tables that have parent-child relationships (using `id`/`parent_id` columns):

- **`\du [table] [id]`** - Drill up: Recursively traverse up the hierarchy from a given row
- **`\dd [table] [id]`** - Drill down: Query immediate children of a given row
- **`\dk [table] [kode]`** - Drill down by kode: Navigate hierarchical data using dot-separated kode paths (e.g., "1.2.3")
- **`\do [table] [id|order by...]`** - Drill one: Get single row by ID, or query with ORDER BY and optional LIMIT
  - `\do users` - Get all rows with LIMIT 100
  - `\do users 123` - Get row with id = 123
  - `\do users order by created_at desc` - Get rows sorted (auto-adds LIMIT 100)
  - `\do users order by created_at desc limit 50` - Custom limit
- **`\tree [table] [root_id]`** - Display hierarchical tree structure with visual indentation

## Schema and Table Introspection

- **`\gcol [table]`** - Get columns list for a table with data types from information_schema
  - Supports schema-qualified names: `\gcol schema.table`
- **`\dc [table] [columns]`** - Get distinct count grouped by specified columns
  - Example: `\dc users country city` shows count of users per country/city combination
- **`\sct [table]`** - Show create table in external terminal window (uses pg_dump)
  - Displays complete DDL in floating i3wm terminal with syntax highlighting

## Data Manipulation

- **`\lt '<path>' [table]`** - Load CSV data from file into table using PostgreSQL's `\copy`
  - File path must be in single quotes
  - CSV format with header row expected
  - Example: `\lt '/tmp/data.csv' users`

## Output Formatting

- **`\df [recipe]`** - Directed format: Set pager and output format recipes
  - Recipe A: visidata-db pager with CSV format (default)
  - Recipe C: no pager with ASCII format

## Custom Key Bindings

- **`Ctrl-E`** - Edit current input in external editor
- **`Ctrl-B`** - Interactive schema selector using rofi
  - Lists all schemas (excluding pg_* and information_schema)
  - Custom sorting: alphabetical for letters, reverse numerical for digits (recent years first)
  - Persists last selected schema to `~/.cache/rlocal/db/{DBCONFIG_ID}.last_schema`
  - Auto-generates and executes `SET search_path TO` statement

## Connection Management

- **Connection keepalive thread** - Background daemon that pings PostgreSQL connection every 30 seconds to prevent timeout during long idle periods
- **`-s/--search-path`** - Command-line argument to set initial search_path on connection
  - Example: `pgcli -s myschema mydb`
- **Prompt search_path display** - Use `\s` placeholder in prompt format to show current search_path
  - Automatically filters out pg_catalog from display
  - Shows comma-separated list of schemas

## Safety Features

- **UPDATE query validation** - All UPDATE queries must have a WHERE clause
  - Throws `UnsafeUpdateError` exception if WHERE clause is missing
  - Prevents accidental mass updates affecting all table rows

## Pager Integration

- **Command scheduling from pager** - Integration with visidata-db for interactive drill operations
  - Tracks last tabular command context using `@reed_tabular_command` decorator
  - Reads reply file at `/tmp/rlocal/visidata/last-reply` after pager closes
  - Supports `drill_up.<id>` and `drill_down.<id>` reply formats
  - Auto-executes corresponding `\du` or `\dd` commands based on pager interaction
- **RVISIDATA integration** - Conditionally skips visidata pager for non-data queries
  - Sets `RVISIDATA_SKIP` environment variable based on query properties
  - Skips for: errors, metadata changes, database changes, search path changes, data mutations

## Autocompletion

Custom commands support intelligent autocompletion:

- Table name completion for all drill commands
- Schema-qualified table name support
- Column name completion for `\dc` command after table name
- Uses existing pgcli completion infrastructure (Schema, Table, Column classes)

## Environment Variables

- **`USE_MINIMAL_COLUMN_SET`** - When set to "1", filters columns to minimal set: `id`, `parent_id`, `level`, `kode`, `code`, `nama`, `name`
- **`PAGER`** - Can be set to "visidata-db" for custom tabular data viewing
- **`DBCONFIG_ID`** - Used for schema persistence cache file naming
- **`RVISIDATA_SKIP`** - Set automatically to skip visidata for non-data queries

## External Tool Integration

This fork integrates with several external tools for enhanced workflows:

- **rofi** - Schema selection menu
- **i3-msg** - Window management integration for \sct command
- **pg_dump** - Table DDL extraction for \sct command
- **visidata-db** - Custom pager for tabular data with interactive drill operations
- **less** - Fallback pager for text content
