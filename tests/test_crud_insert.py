#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import time
import random
from sqlalchemy.exc import IntegrityError
from sqlalchemy_mate.tests import engine, t_smart_insert
from sqlalchemy_mate.crud.insert import smart_insert
from sqlalchemy_mate.crud.select import count_row


def test_smart_insert():
    """

    **中文文档**

    测试smart_insert的基本功能, 以及与普通的insert比较性能。
    """
    ins = t_smart_insert.insert()

    n = 10000
    # Smart Insert
    engine.execute(t_smart_insert.delete())

    data = [{"id": random.randint(1, n)} for i in range(20)]
    for row in data:
        try:
            engine.execute(ins, row)
        except IntegrityError:
            pass
    assert 15 <= count_row(engine, t_smart_insert) <= 20

    data = [{"id": i} for i in range(1, 1 + n)]

    st = time.clock()
    smart_insert(engine, t_smart_insert, data, 5)
    elapse1 = time.clock() - st

    assert count_row(engine, t_smart_insert) == n

    # Regular Insert
    engine.execute(t_smart_insert.delete())

    data = [{"id": random.randint(1, n)} for i in range(20)]
    for row in data:
        try:
            engine.execute(ins, row)
        except IntegrityError:
            pass
    assert 15 <= count_row(engine, t_smart_insert) <= 20

    data = [{"id": i} for i in range(1, 1 + n)]

    st = time.clock()
    for row in data:
        try:
            engine.execute(ins, row)
        except IntegrityError:
            pass
    elapse2 = time.clock() - st

    assert count_row(engine, t_smart_insert) == n

    if n >= 10000:
        assert elapse1 * 5 < elapse2
    else:
        assert elapse1 < elapse2


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
