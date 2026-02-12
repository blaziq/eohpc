#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import os
import shlex

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Self, Tuple, Type, TypeVar

import yaml

DEFAULT_CONFIG_NAME = f"hpc_submit.conf"

CONFIG_GLOBAL = Path(__file__).resolve().parent / f"{DEFAULT_CONFIG_NAME}"
CONFIG_USER = f"~/.config/hpc_submit/{DEFAULT_CONFIG_NAME}"
CONFIG_PROJECT = f"{DEFAULT_CONFIG_NAME}"



def shquote(s: str) -> str:
    return shlex.quote(s)


class ConfigError(RuntimeError):
    pass




# -----------------------------
# Config layering
# -----------------------------

class ConfigParser:
    """Loads YAML layers and deep-merges them (dict-recursive; lists/scalars replaced)."""

    def load_yaml_if_exists(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if data is None:
            return {}
        if not isinstance(data, dict):
            raise ConfigError(f"Config {path} must be a YAML mapping at top-level")
        return data

    def deep_merge(self, base: Any, override: Any) -> Any:
        if isinstance(base, dict) and isinstance(override, dict):
            out = dict(base)
            for k, v in override.items():
                out[k] = self.deep_merge(out[k], v) if k in out else v
            return out
        return override

    def load_merged(self, global_cfg: Path, user_cfg: Path, project_cfg: Optional[Path], cli_overrides: Dict[str, Any]) -> Dict[str, Any]:
        #if not global_cfg.exists():
        #    raise ConfigError(f"Global config missing: {global_cfg}")

        merged: Dict[str, Any] = {}
        merged = self.deep_merge(merged, self.load_yaml_if_exists(global_cfg))
        merged = self.deep_merge(merged, self.load_yaml_if_exists(user_cfg))
        if project_cfg and project_cfg.exists():
            merged = self.deep_merge(merged, self.load_yaml_if_exists(project_cfg))
        merged = self.deep_merge(merged, cli_overrides)
        return merged


# -----------------------------
# CLI overrides
# -----------------------------

class CliOverrideParser:
    """Parses repeated --set key=value into nested dict via dotted keys; values parsed as YAML scalars."""
    def parse(self, items: list[str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for item in items:
            if "=" not in item:
                raise ConfigError(f"--set expects KEY=VALUE, got: {item!r}")
            key, value = item.split("=", 1)
            key = key.strip()
            if not key:
                raise ConfigError(f"Empty key in --set: {item!r}")

            try:
                parsed_value = yaml.safe_load(value)
            except Exception:
                parsed_value = value

            cur = out
            parts = key.split(".")
            for p in parts[:-1]:
                if p not in cur or not isinstance(cur[p], dict):
                    cur[p] = {}
                cur = cur[p]
            cur[parts[-1]] = parsed_value
        return out


# -----------------------------
# Top-level config
# -----------------------------


@dataclass(frozen=True, kw_only=True)
class BaseConfig:
    project: Path
    mode: str
    project_dir: Path
    executable: str
    data_dir: Path = ""
    output_dir: Path = ""
    image: Path = ""
    requirements: str = ""
    venv: str = ""

    @classmethod
    def _req(cls, d: Dict[str, Any], key: str) -> Any:
        if key not in d or d[key] in (None, ""):
            raise ConfigError(f"Missing required config key: {key}")
        return d[key]
    
    @classmethod
    def parse(cls, merged: Dict[str, Any]) -> Dict[str, Any]:
        # returns kwargs for cls(**kwargs) in child classes
        return dict(
            project_dir = Path(str(cls._req(merged, "project_dir"))).expanduser(),
            data_dir = Path(str(cls._req(merged, "data_dir"))).expanduser(),
            output_dir = Path(str(cls._req(merged, "output_dir"))).expanduser(),
            image = Path(str(cls._req(merged, "image"))).expanduser(),
            executable = str(cls._req(merged, "executable")),
            requirements = str(merged.get("requirements") or ""),
            venv = str(merged.get("venv") or ""),
            project = merged.get("project"),
            mode = str(merged.get("mode")),
        )

    @classmethod
    def from_merged(cls, merged: Dict[str, Any]) -> Self:
        base = cls.parse(merged)
        return cls(**base)


C = TypeVar("C", bound=BaseConfig)


class BaseBackend:
    def __init__(self, config: C, writer: ArtifactWriter):
        self.config = config
        self.writer = writer
#        self.top = TopConfig.from_dict(merged)
#        self.payload = ContainerCommandBuilder(self.top).bash_lc_payload()

    def generate(self) -> None:
        raise NotImplementedError


class ContainerCommandBuilder:
    """
    Builds a bash -lc payload to run inside container.
    Binds expected:
      host project_dir -> /project
      host data_dir    -> /data
      host output_dir  -> /output
    """
    def __init__(self, config: C):
        self.cpnfig = config

    def build_bash_lc_payload(self) -> str:
        exe_path = f"/project/{self.top.executable}"
        run_cmd = f"python3 {shquote(exe_path)}" if self.top.executable.endswith(".py") else shquote(exe_path)

        venv_steps = ""
        if self.top.requirements:
            venv_dir = self.top.venv.strip() if self.top.venv.strip() else "/output/.venv"
            req_file = f"/project/{self.top.requirements}"
            venv_steps = f"""
VENV_DIR={shquote(venv_dir)}
REQ_FILE={shquote(req_file)}
if [ -f "$REQ_FILE" ]; then
  python3 -m venv "$VENV_DIR"
  . "$VENV_DIR/bin/activate"
  python3 -m pip install --upgrade pip
  python3 -m pip install -r "$REQ_FILE"
fi
""".strip()

        script = (venv_steps + "\n" + run_cmd).strip() if venv_steps else run_cmd
        return shquote(script)
    


class ArtifactWriter:
    def __init__(self, outdir: Path):
        self.outdir = outdir
        self.outdir.mkdir(parents=True, exist_ok=True)

    def write_text(self, name: str, content: str, mode: int = 0o644) -> Path:
        p = self.outdir / name
        p.write_text(content, encoding="utf-8")
        os.chmod(p, mode)
        return p

# -----------------------------
# Dynamic backend loader
# -----------------------------

def load_backend_classes(mode: str) -> Tuple[Type, Type]:
    """
    Imports backends/<mode>.py and retrieves:
      <Mode>Config and <Mode>Backend
    """
    mode = mode.strip()
    module_name = f"backends.{mode.lower()}"
    module = importlib.import_module(module_name)

    prefix = mode.capitalize()
    config_class_name = f"{prefix}Config"
    backend_class_name = f"{prefix}Backend"

    try:
        ConfigClass = getattr(module, config_class_name)
        BackendClass = getattr(module, backend_class_name)
    except AttributeError as e:
        raise ConfigError(
            f"Plugin {module_name} must define classes {config_class_name} and {backend_class_name}"
        ) from e

    return ConfigClass, BackendClass

# -----------------------------
# Main
# -----------------------------

def resolve_project_cfg(args) -> Optional[Path]:
    if args.project_config:
        return Path(args.project_config).expanduser()
    else:
        return Path(args.project).expanduser() / CONFIG_PROJECT


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Generate submit artifacts for HTCondor or SpaceHPC (plugin-based).")
    ap.add_argument("mode", help='Mode: "htcondor" or "spacehpc"')
    ap.add_argument("project", help='Path to the project')
    ap.add_argument("--project-config", default="")
    ap.add_argument("--set", action="append", default=[], help="Override config key via KEY=VALUE (repeatable). Dotted keys supported.")
    ap.add_argument("--outdir", default="", help="Artifacts output dir (default: <project_dir>/.hpc_submit_gen)")
    
    args = ap.parse_args(argv)
    
    project = Path(args.project).expanduser()
    mode = args.mode.strip().lower()
    outdir = Path(args.outdir).expanduser() if args.outdir else (project / ".hpc_submit_gen")
    outdir.mkdir(parents=True, exist_ok=True)

    global_cfg = Path(CONFIG_GLOBAL).expanduser()
    user_cfg = Path(CONFIG_USER).expanduser()
    project_cfg = resolve_project_cfg(args)

    overrides = CliOverrideParser().parse(args.set)
    merged = ConfigParser().load_merged(global_cfg, user_cfg, project_cfg, overrides)
    merged.update({
        "project": project,
        "mode": mode
    })
    print(merged)
    
    ConfigClass, BackendClass = load_backend_classes(args.mode)
    config = ConfigClass.from_merged(merged)
    writer = ArtifactWriter(outdir)
    backend = BackendClass(config, writer)
    
    backend.generate()

    print(str(outdir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
