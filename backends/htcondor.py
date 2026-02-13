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

    PREFIX = "htcondor"

    def _generate_sub(self, **kwargs) -> None:
        singularity_bind = ",".join(self._get_singularity_binds())
        sh = Path(kwargs.get("sh")).absolute()
        job_sub = f"""
universe              = vanilla
executable            = {sh}
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
+SingularityImage     = {self.config.image}
+SingularityBind      = {singularity_bind}

PROJECT_DIR           = {self.MNT_PROJECT}
DATA_DIR              = {self.MNT_DATA}
OUTPUT_DIR            = {self.MNT_OUTPUT}/$(Cluster)
environment           = PROJECT_DIR=$(PROJECT_DIR);DATA_DIR=$(DATA_DIR);OUTPUT_DIR=$(OUTPUT_DIR)

queue { f"args from {self.config.inputs.absolute()}" if self.config.inputs else ""}
"""
        job_file = self._filename(self.FILE_JOB)
        return self.writer.write_text(job_file, job_sub)

        
    def _generate_sh(self):
        cmd = f"{"python3 " if self.config.executable.endswith(".py") else ""}{self.MNT_PROJECT}/{self.config.executable}"
        source_venv = f"source {self.MNT_PROJECT}/{self.config.venv}" if self.config.venv else ""
        script = f"""
#!/bin/bash

RAW_LINE="$@"
mapfile -t ARGS < <(
python3 - "$RAW_LINE" <<'PY'
import os, sys, shlex
line = sys.argv[1].strip()
if line and not line.startswith("#"):
    expanded = os.path.expandvars(line)
    for arg in shlex.split(expanded):
        print(arg)
PY
)

mkdir -p ${{OUTPUT_DIR}}
{source_venv}
{cmd} "${{ARGS[@]}}"
"""
        script_file = self._filename(self.FILE_SH)
        self.writer.write_text(script_file, script, mode=0o755)
  

    def _generate_htcondor_submit(self, **kwargs) -> Path:
        venv = kwargs.get("venv")
        sub = kwargs.get("sub")
        script = f"""
#!/bin/bash
echo "Submitting job(s)..."
{venv.absolute() if venv else ""}
condor_submit -pool {self.config.pool} -name {self.config.schedd} {sub.absolute()}
""" 
        script_file = self._filename(self.FILE_SUBMIT)
        return self.writer.write_text(script_file, script, mode=0o755)


    def generate(self) -> Path:
        venv = self._generate_venv()
        sh = self._generate_sh()
        print(venv, sh)
        sub = self._generate_sub(sh=sh)
        submit_script = self._generate_htcondor_submit(venv=venv, sub=sub)
        return submit_script
