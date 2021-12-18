# -*- coding: utf-8 -*-

import pytest

from sqlalchemy_mate.crud import selecting
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, t_user, t_inv, t_smart_insert, BaseTest
)


class SelectingApiBaseTest(BaseTest):
    @classmethod
    def class_level_data_setup(cls):
        cls.delete_all_data_in_core_table()

        t_user_data = [
            {"user_id": 1, "name": "Alice"},
            {"user_id": 2, "name": "Bob"},
            {"user_id": 3, "name": "Cathy"},
        ]

        t_inv_data = [
            {"store_id": 1, "item_id": 1},
            {"store_id": 1, "item_id": 2},
            {"store_id": 2, "item_id": 1},
            {"store_id": 2, "item_id": 2},
        ]

        t_smart_insert_data = [
            {"id": i}
            for i in range(1, 1000 + 1)
        ]
        with cls.engine.connect() as connection:
            connection.execute(t_user.insert(), t_user_data)
            connection.execute(t_inv.insert(), t_inv_data)
            connection.execute(t_smart_insert.insert(), t_smart_insert_data)

    @classmethod
    def class_level_data_teardown(cls):
        cls.delete_all_data_in_core_table()

    def test_count_row(self):
        assert selecting.count_row(self.engine, t_user) == 3

    def test_by_pk(self):
        row = selecting.by_pk(self.engine, t_user, 1)
        assert row._fields == ("user_id", "name")
        assert tuple(row) == (1, "Alice")
        assert row._asdict() == {"user_id": 1, "name": "Alice"}

        row = selecting.by_pk(self.engine, t_user, (1,))
        assert row._asdict() == {"user_id": 1, "name": "Alice"}

        assert selecting.by_pk(self.engine, t_user, 0) is None

        row = selecting.by_pk(self.engine, t_inv, (1, 2))
        assert row._asdict() == {"store_id": 1, "item_id": 2}

        with pytest.raises(ValueError):
            selecting.by_pk(self.engine, t_user, (1, 1))

        with pytest.raises(ValueError):
            selecting.by_pk(self.engine, t_inv, 12)

        with pytest.raises(ValueError):
            selecting.by_pk(self.engine, t_inv, (1, 2, 1, 2))

    def test_select_all(self):
        rows = selecting.select_all(self.engine, t_user).all()
        assert len(rows) == 3

        for tp in selecting.yield_tuple(selecting.select_all(self.engine, t_user)):
            assert isinstance(tp, tuple)

        for dct in selecting.yield_dict(selecting.select_all(self.engine, t_user)):
            assert isinstance(dct, dict)

    def test_select_single_column(self):
        data = selecting.select_single_column(self.engine, t_user.c.user_id)
        assert data == [1, 2, 3]

        data = selecting.select_single_column(self.engine, t_user.c.name)
        assert data == ["Alice", "Bob", "Cathy"]

    def test_select_many_column(self):
        data = selecting.select_many_column(self.engine, [t_user.c.user_id, ])
        assert data == [(1,), (2,), (3,)]

        data = selecting.select_many_column(
            engine=self.engine,
            columns=[t_user.c.user_id, t_user.c.name]
        )
        assert data == [(1, "Alice"), (2, "Bob"), (3, "Cathy")]

    def test_select_single_distinct(self):
        # postgres db won't return item in its original insertion order with distinct statement
        assert selecting.select_single_distinct(
            engine=self.engine,
            column=t_inv.c.store_id,
        ) in [[1, 2], [2, 1]]

    def test_select_many_distinct(self):
        result = selecting.select_many_distinct(
            engine=self.engine,
            columns=[t_inv.c.store_id, t_inv.c.item_id],
        )
        desired_result = [(1, 1), (1, 2), (2, 1), (2, 2)]
        for row in desired_result:
            assert row in result
        assert len(result) == len(desired_result)

    def test_select_random(self):
        id_set = set(range(1, 1000 + 1))

        results1 = [
            id_
            for (id_,) in selecting.select_random(
                engine=self.engine, table=t_smart_insert, limit=5,
            )
        ]
        assert len(results1) == 5

        results2 = [
            id_
            for (id_,) in selecting.select_random(
                engine=self.engine, columns=[t_smart_insert.c.id], limit=5,
            )
        ]
        assert len(results2) == 5

        # at least one element not the same
        assert sum([
            i1 != i2
            for i1, i2 in zip(results1, results2)
        ]) >= 1

        with pytest.raises(ValueError):
            selecting.select_random(engine=self.engine)

        with pytest.raises(ValueError):
            selecting.select_random(
                engine=self.engine,
                table=t_smart_insert,
                columns=[t_smart_insert.c.id, ],
            )

        with pytest.raises(ValueError):
            selecting.select_random(
                engine=self.engine,
                table=t_smart_insert,
            )

        with pytest.raises(ValueError):
            selecting.select_random(
                engine=self.engine,
                table=t_smart_insert,
                limit=5,
                perc=10,
            )

        with pytest.raises(ValueError):
            selecting.select_random(
                engine=self.engine,
                table=t_smart_insert,
                perc=1000,
            )

        with pytest.raises(ValueError):
            selecting.select_random(
                engine=self.engine,
                table=t_smart_insert,
                perc=-1000,
            )

        self.psql_only_test_case()

    def psql_only_test_case(self):
        id_set = set(range(1, 1000 + 1))

        results = selecting.select_random(
            engine=self.engine, table=t_smart_insert, perc=10,
        ).all()
        assert 50 <= len(results) <= 150
        for (id_,) in results:
            assert id_ in id_set

        results = selecting.select_random(
            engine=self.engine, columns=[t_smart_insert.c.id, ], perc=10,
        ).all()
        assert 50 <= len(results) <= 150
        for (id_,) in results:
            assert id_ in id_set


class TestSelectingApiSqlite(SelectingApiBaseTest):
    engine = engine_sqlite

    def psql_only_test_case(self):
        pass


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestSelectingApiPostgres(SelectingApiBaseTest):
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
