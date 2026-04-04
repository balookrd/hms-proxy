#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export HMS_SMOKE_RUNNER_NAME="scripts/run-real-installation-smoke-simple.sh"
export HMS_SMOKE_DEFAULT_ENV_FILE="${HMS_SMOKE_DEFAULT_ENV_FILE:-${SCRIPT_DIR}/hms-real-installation-smoke.simple.env}"
export HMS_SMOKE_AUTH_OVERRIDE="simple"

exec "${SCRIPT_DIR}/run-real-installation-smoke.sh" "$@"
