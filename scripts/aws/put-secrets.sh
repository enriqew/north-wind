#!/usr/bin/env bash
# Copy the BarentsWatch client id and secret from .env into SSM Parameter Store (SecureString),
# where the live machine reads them at boot. Values go through a 0600 temp file, never the command
# line, and are not printed.
#
#   scripts/aws/put-secrets.sh
set -euo pipefail
# Git Bash would otherwise rewrite the parameter names (/north-wind/...) as Windows paths.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/../.."
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"
for key in BW_CLIENT_ID BW_CLIENT_SECRET; do
    name="/north-wind/$(echo "$key" | sed 's/^BW_/bw_/' | tr 'A-Z' 'a-z')"
    sed -n "s/^$key=//p" .env | tr -d '\r\n' > "$tmp"
    [ -s "$tmp" ] || { echo "$key is empty in .env"; exit 1; }
    # A native (Windows) AWS CLI cannot read an MSYS path; cygpath turns it into one it can.
    aws ssm put-parameter --region eu-north-1 --name "$name" --type SecureString \
        --value "file://$(cygpath -w "$tmp" 2>/dev/null || echo "$tmp")" --overwrite > /dev/null
    echo "stored $name"
done
