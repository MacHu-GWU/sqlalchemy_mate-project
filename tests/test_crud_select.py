#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from sqlalchemy_mate.crud import select
from sqlalchemy_mate.tests import engine, t_user, t_inv


def test_count_row():
    assert select.count_row(engine, t_user) == 3


def test_select_column():
    header, user_id_list = select.select_column(engine, t_user.c.user_id)
    assert header == "user_id"
    assert user_id_list == [1, 2, 3]

    headers, data = select.select_column(
        engine, t_user.c.user_id, t_user.c.name)
    assert headers == ["user_id", "name"]
    assert data == [(1, "Jack"), (2, "Mike"), (3, "Paul")]


def test_select_distinct_column():
    assert select.select_distinct_column(engine, t_inv.c.store_id) == [1, 2]

    assert select.select_distinct_column(
        engine, t_inv.c.store_id, t_inv.c.item_id
    ) == [[1, 1], [1, 2], [2, 1], [2, 2]]


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
