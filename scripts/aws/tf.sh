#!/usr/bin/env bash
# Terraform in a container, with the current AWS session's credentials, so nothing is installed.
#
#   scripts/aws/tf.sh <storage|run> <terraform args...>
#
# Needs a live AWS CLI session (`aws login`). The credentials go into the container's environment
# for this one command and are not written anywhere.
set -euo pipefail
export MSYS_NO_PATHCONV=1

dir=$1
shift
cd "$(dirname "$0")/../.."
# The Windows CLI ends its lines with \r, which would end up inside the session token.
creds=$(aws configure export-credentials --format env-no-export | tr -d '\r')
env_args=()
while IFS= read -r line; do
    [ -n "$line" ] && env_args+=(-e "$line")
done <<< "$creds"

docker run --rm -i "${env_args[@]}" -e AWS_REGION=eu-north-1 \
    -v "$PWD/infra:/infra" -w "/infra/$dir" \
    hashicorp/terraform:1.9.8 "$@"
