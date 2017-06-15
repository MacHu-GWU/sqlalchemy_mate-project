#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import pytest
from sqlalchemy import select
from sqlalchemy_mate.tests import engine, t_user, t_inv
from sqlalchemy_mate import pt


def test():
    t = pt.from_sql(select([t_user]), engine)
#     print(t)

    t = pt.from_table(t_inv, engine)
#     print(t)


if __name__ == "__main__":
    import os
    pytest.main([os.path.basename(__file__), "--tb=native", "-s", ])
