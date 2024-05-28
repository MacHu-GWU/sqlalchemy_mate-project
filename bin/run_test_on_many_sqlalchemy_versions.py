# -*- coding: utf-8 -*-

import subprocess
from sqlalchemy_mate.paths import dir_project_root, dir_venv_bin
from rich import print as rprint
from rich.panel import Panel

sqlalchemy_versions = [
    "2.0.0",
    "2.0.4",
    "2.0.8",
    "2.0.12",
    "2.0.16",
    "2.0.20",
    "2.0.24",
    "2.0.28",
]
path_venv_pip = dir_venv_bin / "pip"
with dir_project_root.cwd():
    for version in sqlalchemy_versions:
        rprint(Panel(f"Test on sqlalchemy.__version__ = {version}"))
        subprocess.run(
            [
                f"{path_venv_pip}",
                "install",
                f"sqlalchemy=={version}",
            ]
        )
        subprocess.run(["pyops", "cov-only"], check=True)
