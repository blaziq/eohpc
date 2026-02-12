from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from hpc_submit import BaseConfig, BaseBackend, ConfigError  # import from main module

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

    def _generate_sub(self) -> None:
        pass

    def generate(self) -> None:
        # Binds: host -> container
        binds = [
            f"{self.top.project_dir}:/project",
            f"{self.top.data_dir}:/data",
            f"{self.top.output_dir}:/output",
        ]
        bind_value = ",".join(binds)

        # Ensure output dir exists on shared FS
        self.top.output_dir.mkdir(parents=True, exist_ok=True)

        # Important: arguments string needs careful quoting.
        # self.payload is already shell-quoted for bash -lc.
        # We want arguments: -lc '<payload>'
        # So we pass: arguments = -lc '<payload without surrounding quotes?>'
        # Easiest: keep the single-quoted payload, but wrap the whole in double-quotes in submit file.
        # We’ll emit:
        #   arguments = -lc <payload>
        # where payload already includes quotes.
        submit = []
        submit.append("universe = vanilla")
        submit.append("executable = /bin/bash")
        submit.append(f"arguments  = -lc {self.payload}")
        submit.append("transfer_executable = False")
        submit.append("should_transfer_files = NO")
        submit.append("when_to_transfer_output = ON_EXIT")
        submit.append(f"request_cpus   = {ht.cpus}")
        submit.append(f"request_gpus   = {ht.gpus}")
        submit.append(f"request_memory = {ht.ram}")
        submit.append(f"output = {self.writer.outdir}/condor.$(Cluster).$(Process).out")
        submit.append(f"error  = {self.writer.outdir}/condor.$(Cluster).$(Process).err")
        submit.append(f"log    = {self.writer.outdir}/condor.$(Cluster).log")
        submit.append("")
        submit.append(f"+SingularityJob = True")
        submit.append(f'+SingularityImage = "{self.top.image}"')
        submit.append(f'+SingulartiyBind = "{bind_value}"')
        #if ht.extra_args.strip():
        #    submit.append(f'+SingularityArguments = "{ht.extra_args.strip()}"')

        if ht.pool:
            submit.append(f"pool = {ht.pool}")
        if ht.name:
            submit.append(f"name = {ht.name}")

        submit.append("queue")

        self.writer.write_text("job.sub", "\n".join(submit) + "\n")
