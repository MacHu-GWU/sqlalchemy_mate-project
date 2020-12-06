# -*- coding: utf-8 -*-

import pytest

from sqlalchemy_mate.crud import selecting
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, t_user, t_inv, BaseClassForTest
)


class SelectingApiBaseTest(BaseClassForTest):
    @classmethod
    def class_level_data_setup(cls):
        cls.engine.execute(t_user.delete())
        data = [{"user_id": 1, "name": "Alice"},
                {"user_id": 2, "name": "Bob"},
                {"user_id": 3, "name": "Cathy"}]
        cls.engine.execute(t_user.insert(), data)

        cls.engine.execute(t_inv.delete())
        data = [{"store_id": 1, "item_id": 1},
                {"store_id": 1, "item_id": 2},
                {"store_id": 2, "item_id": 1},
                {"store_id": 2, "item_id": 2}]
        cls.engine.execute(t_inv.insert(), data)

    def test_count_row(self):
        assert selecting.count_row(self.engine, t_user) == 3

    def test_select_single_column(self):
        header, data = selecting.select_single_column(self.engine, t_user.c.user_id)
        assert (header, data) == ("user_id", [1, 2, 3])

        header, data = selecting.select_single_column(self.engine, t_user.c.name)
        assert (header, data) == ("name", ["Alice", "Bob", "Cathy"])

    def test_select_many_column(self):
        # headers, data = selecting.select_many_column(engine, t_user.c.user_id)
        # assert (headers, data) == (["user.user_id", ], [(1,), (2,), (3,)])

        headers, data = selecting.select_many_column(self.engine, [t_user.c.user_id, ])
        assert (headers, data) == ([f"{t_user.name}.user_id", ], [(1,), (2,), (3,)])

        headers, data = selecting.select_many_column(
            self.engine, t_user.c.user_id, t_user.c.name)
        assert (headers, data) == (
            [f"{t_user.name}.user_id", f"{t_user.name}.name"],
            [(1, "Alice"), (2, "Bob"), (3, "Cathy")],
        )

        headers, data = selecting.select_many_column(
            self.engine, [t_user.c.user_id, t_user.c.name])
        assert (headers, data) == (
            [f"{t_user.name}.user_id", f"{t_user.name}.name"],
            [(1, "Alice"), (2, "Bob"), (3, "Cathy")],
        )

    def test_select_distinct_column(self):
        # postgres db won't return item in its original insertion order with distinct statement
        assert selecting.select_distinct_column(self.engine, t_inv.c.store_id) in [[1, 2], [2, 1]]
        result = selecting.select_distinct_column(
            self.engine, t_inv.c.store_id, t_inv.c.item_id
        )
        desired_result = [(1, 1), (1, 2), (2, 1), (2, 2)]
        for row in desired_result:
            assert row in result
        assert len(result) == len(desired_result)

    def test_select_random(self):
        user_id_set = {1, 2, 3}
        for (user_id, _) in selecting.select_random(self.engine, [t_user], limit=1):
            assert user_id in user_id_set

        for (user_id,) in selecting.select_random(self.engine, [t_user.c.user_id], limit=1):
            assert user_id in user_id_set


class TestSelectingApiSqlite(SelectingApiBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(IS_WINDOWS,
                    reason="no psql service container for windows")
class TestSelectingApiPostgres(SelectingApiBaseTest):
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
