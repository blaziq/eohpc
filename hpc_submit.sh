#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-htcondor}"
shift || true

OUTDIR="$(python3 ./hpc_submit.py "$MODE" "$@")"
echo "Generated in: $OUTDIR"

if [ "$MODE" = "spacehpc" ]; then
  exec "$OUTDIR/submit_spacehpc.sh"
else
  exec condor_submit "$OUTDIR/job.sub"
fi
