#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import pytest
from sqlalchemy_mate.tests import engine, t_user
from sqlalchemy_mate import io


def teardown_module(module):
    import os

    try:
        filepath = __file__.replace("test_io.py", "t_user.csv")
        os.remove(filepath)
    except:
        pass


def test_table_to_csv():
    filepath = __file__.replace("test_io.py", "t_user.csv")
    io.table_to_csv(t_user, engine, filepath, chunksize=1, overwrite=True)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
