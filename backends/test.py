from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from hpc_submit import BaseConfig, BaseBackend, ConfigError  # import from main module

@dataclass(frozen=True)
class TestConfig(BaseConfig):

    @classmethod
    def parse(cls, merged: Dict[str, Any]) -> Dict[str, Any]:
        base = super().parse(merged)
        base.update({"a": "xyz-abc"})
        return base
    

class TestBackend(BaseBackend):

    def generate(self):
        print(self.config)

    