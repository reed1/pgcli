# reed's Custom Command Auto-Completion

This adds table name auto-completion support for reed's custom pgcli commands.

## Supported Commands

The following commands now support table name auto-completion:

- `\do` - Get one row from table
- `\dd` - Drill down a table  
- `\du` - Drill up a table
- `\dk` - Drill down a table by dot-joined kode
- `\tree` - Print tree of a table
- `\gcol` - Get columns of a table
- `\dc` - Get distinct column values count (supports multiple column completion for GROUP BY)
- `\sct` - Show create table

## How it works

1. When you type any of these commands followed by a space, pgcli will suggest table names
2. Schema-qualified tables are supported (e.g., `\sct public.users`)
3. For the `\dc` command, after you've typed the table name, it will suggest column names for each additional argument

## Implementation

- Minimal changes to vendor code (`pgcli/packages/sqlcompletion.py`)
- Main logic in `pgcli/reed_commands.py` using functions `is_reed_command()` and `reed_suggestions()`
- Uses the existing pgcli completion framework

## Examples

```
\do <TAB>              # Shows table suggestions
\sct public.<TAB>      # Shows tables in public schema  
\dc users <TAB>        # Shows column suggestions for users table
\dc users country <TAB> # Shows more column suggestions for additional GROUP BY columns
```