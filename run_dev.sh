#!/bin/bash

set -euo pipefail

temp_dir=$(mktemp -d /tmp/test_proj_pgcli.XXXXXX)

script_dir=$(readlink -f "$(dirname "$0")")

cat <<EOF > "$temp_dir/pgcli"
#!/bin/bash

cd "$script_dir"
python -m pgcli.main "\$@"
EOF

chmod +x "$temp_dir/pgcli"

export PATH="$temp_dir":"$PATH"

db "$@"

rm -rf "$temp_dir"
