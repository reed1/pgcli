# pgcli Fork

This is a customized fork of [pgcli](https://github.com/dbcli/pgcli) with enhanced features for database exploration, hierarchical data navigation, and improved workflow integration.

---

## Shared Features

> **⚠️ NOTE**: This section is identical in both the [mycli fork](https://github.com/dbcli/mycli) and [pgcli fork](https://github.com/dbcli/pgcli). When updating features here, please keep both README files synchronized.

### Hierarchical Data Navigation

Commands for working with tables that have parent-child relationships (using `id`/`parent_id` columns):

- **`\du [table] [id]`** - Drill up: Recursively traverse up the hierarchy from a given row
- **`\dd [table] [id]`** - Drill down: Query immediate children of a given row
- **`\ddr [table] [id] [where ...]`** - Drill down recursive: Recursively traverse down the hierarchy from a given row
  - `\ddr categories 1` - Get all descendants of row with id = 1
  - `\ddr categories 1 where level < 3` - Get descendants with additional WHERE conditions
- **`\dk [table] [kode]`** - Drill down by kode: Navigate hierarchical data using dot-separated kode paths (e.g., "01.02.03")
- **`\do [table] [id|order by...]`** - Drill one: Get single row by ID, or query with ORDER BY and optional LIMIT
  - `\do users` - Get all rows with LIMIT 100
  - `\do users 123` - Get row with id = 123
  - `\do users order by created_at desc` - Get rows sorted (auto-adds LIMIT 100)
  - `\do users order by created_at desc limit 50` - Custom limit
- **`\tree [table] [root_id]`** - Display hierarchical tree structure with visual indentation

### Schema and Table Introspection

- **`\gcol [table]`** - Get columns list for a table with data types from information_schema
- **`\dc [table] [columns]`** - Get distinct count grouped by specified columns
  - Example: `\dc users country city` shows count of users per country/city combination
- **`\sct [table]`** - Show create table in external terminal window
  - Displays complete DDL in floating i3wm terminal with syntax highlighting

### Data Manipulation

- **`\lt '<path>' [table]`** - Load CSV data from file into table
  - File path must be in single quotes
  - CSV format with header row expected
  - Example: `\lt '/tmp/data.csv' users`
- **`\tc [table]`** - Truncate table

### Output Formatting

- **`\df [recipe]`** - Directed format: Set pager and output format recipes
  - Recipe A: visidata-db pager with CSV format (default)
  - Recipe C: no pager with ASCII format

### Custom Key Bindings

- **`Ctrl-E`** - Edit current input in external editor
- **`Ctrl-B`** - Interactive schema selector using rofi
  - Custom sorting: alphabetical for letters, reverse numerical for digits (recent years first)
  - Persists last selected schema to `~/.cache/rlocal/db/{DBCONFIG_ID}.last_schema`
  - Auto-generates and executes schema switch statement

### Connection Management

- **Connection keepalive thread** - Background daemon that pings database connection every 30 seconds to prevent timeout during long idle periods

### Safety Features

- **UPDATE query validation** - All UPDATE queries must have a WHERE clause
  - Throws `UpdateWithoutWhereError` exception if WHERE clause is missing
  - Prevents accidental mass updates affecting all table rows

### Pager Integration

- **Drill from visidata-db** - Run tabular command → select row → press `{` (drill up) or `}` (drill down) → auto-executes `\du`/`\dd` in CLI

### Autocompletion

Custom commands support intelligent autocompletion:

- Table name completion for all drill commands
- Column name completion for `\dc` command after table name
- Uses existing CLI completion infrastructure

### Environment Variables

- **`USE_MINIMAL_COLUMN_SET`** - When set to "1", filters columns to minimal set: `id`, `parent_id`, `level`, `kode`, `code`, `nama`, `name`
- **`PAGER`** - Can be set to "visidata-db" for custom tabular data viewing
- **`DBCONFIG_ID`** - Used for schema persistence cache file naming

### External Tool Integration

This fork integrates with several external tools for enhanced workflows:

- **rofi** - Schema selection menu
- **i3-msg** - Window management integration for `\sct` command
- **visidata-db** - Custom pager for tabular data with interactive drill operations

---

## PostgreSQL-Specific Features

### Schema Support

- **`\gcol [table]`** - Supports schema-qualified names: `\gcol schema.table`
- **`Ctrl-B`** schema selector:
  - Lists all schemas (excluding pg_* and information_schema)
  - Auto-generates and executes `SET search_path TO` statement
- **`-s/--search-path`** - Command-line argument to set initial search_path on connection
  - Example: `pgcli -s myschema mydb`
- **Prompt search_path display** - Use `\s` placeholder in prompt format to show current search_path
  - Automatically filters out pg_catalog from display
  - Shows comma-separated list of schemas

### Autocompletion

- **Schema-qualified table name support** - Completion works with schema.table syntax
- Uses pgcli completion infrastructure (Schema, Table, Column classes)

### Implementation Details

- **`\lt`** - Uses PostgreSQL's `\copy` command for CSV loading
- **`\sct`** - Queries `information_schema.columns` and `pg_indexes` for fast table structure display
  - Generates CREATE TABLE statement with column definitions and CREATE INDEX statements
  - Single query execution for optimal performance
- **`\sctd`** - Uses `pg_dump` to extract complete table DDL
  - Alternative to `\sct` that provides full DDL including constraints, triggers, and additional metadata
  - Slower but more comprehensive than `\sct`

### Additional Tools

- **pg_dump** - Table DDL extraction
- **less** - Fallback pager for text content
