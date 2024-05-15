# -*- coding: utf-8 -*-

import pytest

from sqlalchemy_mate.crud import selecting
from sqlalchemy_mate.crud import updating
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    t_cache,
    t_graph,
    BaseTest,
)


class UpdatingApiBaseTest(BaseTest):
    def teardown_method(self, method):
        """
        Make sure data in all table is cleared after each test cases.
        """
        self.delete_all_data_in_core_table()

    def test_upsert_all_single_primary_key_column(self):
        # ------ Before State ------
        ins = t_cache.insert()
        data = [
            {"key": "a1"},
        ]
        with self.engine.connect() as connection:
            connection.execute(ins, data)
            connection.commit()

        # Upsert all
        data = [
            {"key": "a1", "value": 1},  # This will update
            {"key": "a2", "value": 2},  # This will insert
            {"key": "a3", "value": 3},  # This will insert
        ]
        # ------ Invoke ------
        update_counter, insert_counter = updating.upsert_all(self.engine, t_cache, data)
        assert update_counter == 1
        assert insert_counter == 2

        # ------ After State ------
        assert list(selecting.select_all(self.engine, t_cache)) == [
            ("a1", 1),
            ("a2", 2),
            ("a3", 3),
        ]

    def test_upsert_all_multiple_primary_key_column(self):
        # ------ Before State ------
        ins = t_graph.insert()
        data = [
            {"x_node_id": 1, "y_node_id": 2, "value": 0},
        ]
        with self.engine.connect() as connection:
            connection.execute(ins, data)
            connection.commit()

        # ------ Invoke ------
        data = [
            {"x_node_id": 1, "y_node_id": 2, "value": 2},  # This will update
            {"x_node_id": 1, "y_node_id": 3, "value": 3},  # This will insert
            {"x_node_id": 1, "y_node_id": 4, "value": 4},  # This will insert
        ]
        update_counter, insert_counter = updating.upsert_all(self.engine, t_graph, data)
        assert update_counter == 1
        assert insert_counter == 2

        # ------ After State ------
        assert selecting.select_all(self.engine, t_graph).all() == [
            (1, 2, 2),
            (1, 3, 3),
            (1, 4, 4),
        ]


class TestUpdatingApiSqlite(UpdatingApiBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestUpdatingApiPostgres(UpdatingApiBaseTest):
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.crud.updating", preview=False)
