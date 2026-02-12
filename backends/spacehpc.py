from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from hpc_submit import BaseConfig, BaseBackend, ConfigError, shquote

@dataclass(frozen=True)
class SpacehpcConfig(BaseConfig):
    login_node: str = ""
    user: str = ""
    ssh_key: Path = Path()
    project: str = ""
    queue: str = ""
    nodes: int = 1
    cpus: int = 1
    gpus: int = 0
    ram: str = "1G"
    walltime: str = "00:10:00"
    job_name: str = "hpc_submit"

    # where to place the venv on remote (if top-level venv is empty)
    venv_default_remote: str = ".venv"  # relative to REMOTE_PROJECT_ROOT

    @classmethod
    def parse(cls, merged: Dict[str, Any]) -> Dict[str, Any]:
        base = super().parse(merged)
        sp = merged.get("spacehpc", {}) or {}
        
        base.update({"login_node": str(cls._req(sp, "login_node"))})
        base.update({"user": str(cls._req(sp, "user"))})
        base.update({"ssh_key": str(cls._req(sp, "ssh_key"))})
        base.update({"project": str(cls._req(sp, "project"))})
        base.update({"queue": str(sp.get("queue"))})
        base.update({"nodes": int(sp.get("nodes", 1))})
        base.update({"cpus": int(sp.get("cpus", 1))})
        base.update({"gpus": int(sp.get("gpus", 1))})
        base.update({"ram": str(sp.get("ram", "1G"))})
        base.update({"walltime": str(sp.get("walltime", "00:10:00"))})
        base.update({"job_name": int(sp.get("job_name", "hpc_submit"))})
        return base


class SpacehpcBackend(BaseBackend):

    def generate(self) -> None:
        sp = SpacehpcConfig.from_merged(self.merged)

        data_basename = self.top.data_dir.name
        proj_basename = self.top.project_dir.name
        img_basename = self.top.image.name
        out_basename = self.top.output_dir.name

        # PBS script (still uses apptainer directly on remote)
        self.writer.write_text(
            "job.pbs",
            f"""#!/usr/bin/env bash
#PBS -N {sp.job_name}
{"#PBS -q " + sp.queue if sp.queue else ""}
#PBS -l select={sp.nodes}:ncpus={sp.cpus}:ngpus={sp.gpus}:mem={sp.ram}
#PBS -l walltime={sp.walltime}
#PBS -j oe
#PBS -V

set -euo pipefail

: "${{REMOTE_PROJECTS_BASE:?}}"
: "${{REMOTE_SCRATCH_BASE:?}}"
: "${{SPACEHPC_PROJECT:?}}"
: "${{SPACEHPC_USER:?}}"
: "${{REMOTE_PROJ_DIR:?}}"
: "${{REMOTE_DATA_DIR:?}}"
: "${{REMOTE_OUT_DIR:?}}"
: "${{REMOTE_IMAGE:?}}"

cd "$REMOTE_PROJ_DIR"
mkdir -p "$REMOTE_OUT_DIR"

apptainer exec \\
  --bind "$REMOTE_PROJ_DIR:/project" \\
  --bind "$REMOTE_DATA_DIR:/data" \\
  --bind "$REMOTE_OUT_DIR:/output" \\
  "$REMOTE_IMAGE" \\
  bash -lc {self.payload}
""",
            mode=0o755,
        )

        # Remote base dirs:
        scratch_expr = shquote(self.top.scratch_dir) if self.top.scratch_dir else '${SPACEHPC_SCRATCH_BASE:-/scratch}'
        projects_expr = shquote(self.top.projects_dir) if self.top.projects_dir else '${SPACEHPC_PROJECTS_BASE:-/shared/projects}'

        self.writer.write_text(
            "submit_spacehpc.sh",
            f"""#!/usr/bin/env bash
set -euo pipefail

LOGIN_NODE={shquote(sp.login_node)}
USER={shquote(sp.user)}
SSH_KEY={shquote(str(sp.ssh_key))}
PROJECT={shquote(sp.project)}

LOCAL_PROJECT_DIR={shquote(str(self.top.project_dir))}
LOCAL_DATA_DIR={shquote(str(self.top.data_dir))}
LOCAL_IMAGE={shquote(str(self.top.image))}
LOCAL_OUTPUT_DIR={shquote(str(self.top.output_dir))}

REMOTE_SCRATCH_BASE={scratch_expr}
REMOTE_PROJECTS_BASE={projects_expr}

REMOTE_SCRATCH_ROOT="$REMOTE_SCRATCH_BASE/$PROJECT/$USER"
REMOTE_PROJECT_ROOT="$REMOTE_PROJECTS_BASE/$PROJECT/$USER"

REMOTE_DATA_DIR="$REMOTE_SCRATCH_ROOT/{shquote(data_basename)}"
REMOTE_OUT_DIR="$REMOTE_SCRATCH_ROOT/{shquote(out_basename)}"
REMOTE_PROJ_DIR="$REMOTE_PROJECT_ROOT/{shquote(proj_basename)}"
REMOTE_IMAGE_DIR="$REMOTE_PROJECT_ROOT/images"
REMOTE_IMAGE="$REMOTE_IMAGE_DIR/{shquote(img_basename)}"

ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$USER@$LOGIN_NODE" \\
  "mkdir -p \\"$REMOTE_SCRATCH_ROOT\\" \\"$REMOTE_PROJECT_ROOT\\" \\"$REMOTE_IMAGE_DIR\\" \\"$REMOTE_OUT_DIR\\"" >/dev/null

rsync -a --delete -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" \\
  "$LOCAL_DATA_DIR/" "$USER@$LOGIN_NODE:$REMOTE_DATA_DIR/"

rsync -a --delete -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" \\
  "$LOCAL_PROJECT_DIR/" "$USER@$LOGIN_NODE:$REMOTE_PROJ_DIR/"

rsync -a -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new" \\
  "$LOCAL_IMAGE" "$USER@$LOGIN_NODE:$REMOTE_IMAGE"

REMOTE_GEN_DIR="$REMOTE_PROJECT_ROOT/.hpc_submit_gen"
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$USER@$LOGIN_NODE" \\
  "mkdir -p \\"$REMOTE_GEN_DIR\\"" >/dev/null

scp -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new {shquote(str(self.writer.outdir / "job.pbs"))} \\
  "$USER@$LOGIN_NODE:$REMOTE_GEN_DIR/job.pbs" >/dev/null

ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$USER@$LOGIN_NODE" \\
  "qsub -v REMOTE_PROJECTS_BASE=\\"$REMOTE_PROJECTS_BASE\\",REMOTE_SCRATCH_BASE=\\"$REMOTE_SCRATCH_BASE\\",SPACEHPC_PROJECT=\\"$PROJECT\\",SPACEHPC_USER=\\"$USER\\",REMOTE_PROJ_DIR=\\"$REMOTE_PROJ_DIR\\",REMOTE_DATA_DIR=\\"$REMOTE_DATA_DIR\\",REMOTE_OUT_DIR=\\"$REMOTE_OUT_DIR\\",REMOTE_IMAGE=\\"$REMOTE_IMAGE\\" \\
    \\"$REMOTE_GEN_DIR/job.pbs\\"" 
""",
            mode=0o755,
        )
