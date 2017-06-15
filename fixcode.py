#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib_mate import Path

dirpath_list = [
    "sqlalchemy_mate",
    "tests",
]
for dirpath in dirpath_list:
    p = Path(dirpath)
    p.autopep8()