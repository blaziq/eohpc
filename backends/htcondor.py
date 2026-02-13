from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from hpc_submit import BaseConfig, BaseBackend, ConfigError, shquote  # import from main module

@dataclass(frozen=True)
class HtcondorConfig(BaseConfig):
    pool: str = ""
    schedd: str = ""
    cpus: int = 1
    gpus: int = 0
    ram: str = "1G"

    @classmethod
    def parse(cls, merged: Dict[str, Any]) -> Dict[str, Any]:
        base = super().parse(merged)
        ht = merged.get("htcondor", {}) or {}
        base.update({
            "pool": str(cls._req(ht, "pool")),
            "schedd": str(cls._req(ht, "schedd")),
            "cpus": int(ht.get("cpus", cls.cpus)),
            "gpus": int(ht.get("gpus", cls.gpus)),
            "ram": str(ht.get("ram", cls.ram)), 
        })
        return base


class HtcondorBackend(BaseBackend):

    FILE_SUB = "htcondor_job.sub"
    FILE_SH = "htcondor_job.sh"
    FILE_HTCONDOR_SUBMIT = "htcondor_submit.sh"

    MNT_PROJECT = "/project"
    MNT_DATA = "/data"
    MNT_OUTPUT = "/output"

    def _generate_sub(self) -> None:

        singularity_bind = ",".join([
            f"{self.config.project.absolute()}:{self.MNT_PROJECT}",
            f"{self.config.data_dir.absolute()}:{self.MNT_DATA}",
            f"{self.config.output_dir.absolute()}:{self.MNT_OUTPUT}"
        ])

        job_sub = f"""
universe              = vanilla
executable            = {self.writer.outdir}/{self.FILE_SH}
arguments             = $(args)
#transfer_executable   = NO
should_transfer_files = NO
request_cpus          = {self.config.cpus}
{ f"request_gpus          = {self.config.gpus}" if self.config.gpus else "" }
request_memory        = {self.config.ram}
output                = {self.writer.outdir}/$(Cluster).$(Process).out
error                 = {self.writer.outdir}/$(Cluster).$(Process).err
log                   = {self.writer.outdir}/$(Cluster).log

+SingularityJob       = True
+SingularityImage     = "{self.config.image}"
+SingularityBind      = "{singularity_bind}"

PROJECT_DIR           = {self.MNT_PROJECT}
DATA_DIR              = {self.MNT_DATA}
OUTPUT_DIR            = {self.MNT_OUTPUT}/$(Cluster)
environment           = PROJECT_DIR=$(PROJECT_DIR);DATA_DIR=$(DATA_DIR);OUTPUT_DIR=$(OUTPUT_DIR)

queue { f"args from {self.config.inputs.absolute()}" if self.config.inputs else ""}
"""
        self.writer.write_text(self.FILE_SUB, job_sub)

        
    def _generate_sh(self):
        exe_path = f"/project/{self.config.executable}"
        run_cmd = f"python3 {exe_path}" if self.config.executable.endswith(".py") else exe_path

        venv_steps = ""
        if self.config.requirements:
            venv = self.config.venv.strip()
            venv_dir = Path(venv if venv else "${_CONDOR_SCRATCH_DIR}/.venv").expanduser()
            req_file = f"{self.MNT_PROJECT}/{self.config.requirements}"
            venv_steps = f"""
VENV="{venv_dir}"
REQUIREMENTS="{req_file}"
LOCKFILE="$VENV/.lock"

mkdir -p "$VENV"
exec 9>"$LOCKFILE"
flock 9

if [ -f "$REQUIREMENTS" ]; then
    if [ ! -x "$VENV/bin/python" ]; then
        python3 -m venv "$VENV"
    fi
    source $VENV/bin/activate
    python3 -m pip install --upgrade pip
    python3 -m pip install --requirement $REQUIREMENTS
fi

flock -u 9
exec 9>&-
"""

        job_sh = f"""
#!/bin/bash
{venv_steps}

mapfile -t ARGS < <(
python3 - << PY
import os, sys, shlex
s = os.path.expandvars(sys.argv[1])
for a in shlex.split(s):
    print(a)
PY
"$RAW_LINE"
)

{run_cmd} "${{ARGS[@]}}"
"""
        self.writer.write_text(self.FILE_SH, job_sh.strip(), mode=0o755)
  
    def _generate_htcondor_submit(self) -> str:
        script = f"""
#!/bin/bash
condor_submit -pool {self.config.pool} -name {self.config.schedd} {self.writer.outdir}/{self.FILE_SUB}
"""
        self.writer.write_text(self.FILE_HTCONDOR_SUBMIT, script.strip(), mode=0o755)
        return f"{self.writer.outdir}/{self.FILE_HTCONDOR_SUBMIT}"

    def generate(self) -> str:
        self._generate_sub()
        self._generate_sh()
        submit_script = self._generate_htcondor_submit()
        return submit_script
