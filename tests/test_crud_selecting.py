#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from sqlalchemy_mate.crud import selecting
from sqlalchemy_mate.tests import engine, t_user, t_inv


def test_count_row():
    assert selecting.count_row(engine, t_user) == 3


def test_select_single_column():
    header, data = selecting.select_single_column(engine, t_user.c.user_id)
    assert (header, data) == ("user_id", [1, 2, 3])

    header, data = selecting.select_single_column(engine, t_user.c.name)
    assert (header, data) == ("name", ["Alice", "Bob", "Cathy"])


def test_select_many_column():
    # headers, data = selecting.select_many_column(engine, t_user.c.user_id)
    # assert (headers, data) == (["user.user_id", ], [(1,), (2,), (3,)])

    headers, data = selecting.select_many_column(engine, [t_user.c.user_id, ])
    assert (headers, data) == (["user.user_id", ], [(1,), (2,), (3,)])

    headers, data = selecting.select_many_column(
        engine, t_user.c.user_id, t_user.c.name)
    assert (headers, data) == (
        ["user.user_id", "user.name"],
        [(1, "Alice"), (2, "Bob"), (3, "Cathy")],
    )

    headers, data = selecting.select_many_column(
        engine, [t_user.c.user_id, t_user.c.name])
    assert (headers, data) == (
        ["user.user_id", "user.name"],
        [(1, "Alice"), (2, "Bob"), (3, "Cathy")],
    )


def test_select_distinct_column():
    assert selecting.select_distinct_column(engine, t_inv.c.store_id) == [1, 2]
    assert selecting.select_distinct_column(
        engine, t_inv.c.store_id, t_inv.c.item_id
    ) == [(1, 1), (1, 2), (2, 1), (2, 2)]


def test_select_random():
    user_id_set = {1, 2, 3}
    for (user_id, _) in selecting.select_random(engine, [t_user], limit=1):
        assert user_id in user_id_set

    for (user_id,) in selecting.select_random(engine, [t_user.c.user_id], limit=1):
        assert user_id in user_id_set


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
