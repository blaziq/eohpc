#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-htcondor}"
shift || true

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"

OUTDIR="$(python3 $SCRIPT_DIR/hpc_submit.py "$MODE" "$@")"
echo "Generated in: $OUTDIR"

exec ${OUTDIR}/${MODE}_submit.sh

#if [ "$MODE" = "spacehpc" ]; then
#  exec "$OUTDIR/submit_spacehpc.sh"
#else
#  exec condor_submit "$OUTDIR/job.sub"
#fi
