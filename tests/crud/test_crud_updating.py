# -*- coding: utf-8 -*-

import pytest

from sqlalchemy_mate.crud import selecting
from sqlalchemy_mate.crud import updating
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, t_cache, t_graph, BaseClassForTest
)


class UpdatingApiBaseTest(BaseClassForTest):
    def method_level_data_setup(self):
        self.engine.execute(t_cache.delete())
        self.engine.execute(t_graph.delete())

    def test_upsert_all_single_primary_key_column(self):
        # Insert some initial data
        ins = t_cache.insert()
        data = [
            {"key": "a1"},
        ]
        self.engine.execute(ins, data)

        # Upsert all
        data = [
            {"key": "a1", "value": 1},  # This will update
            {"key": "a2", "value": 2},  # This will insert
        ]
        updating.upsert_all(self.engine, t_cache, data)

        assert list(selecting.select_all(self.engine, t_cache)) == [
            ('a1', 1), ('a2', 2)
        ]

    def test_upsert_all_multiple_primary_key_column(self):
        # Insert some initial data
        ins = t_graph.insert()
        data = [
            {"x_node_id": 1, "y_node_id": 2, "value": 0},
        ]
        self.engine.execute(ins, data)

        # Upsert all
        data = [
            {"x_node_id": 1, "y_node_id": 2, "value": 2},  # This will update
            {"x_node_id": 1, "y_node_id": 3, "value": 3},  # This will insert
        ]
        updating.upsert_all(self.engine, t_graph, data)

        assert list(selecting.select_all(self.engine, t_graph)) == [
            (1, 2, 2), (1, 3, 3)
        ]


class TestUpdatingApiSqlite(UpdatingApiBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(IS_WINDOWS,
                    reason="no psql service container for windows")
class TestUpdatingApiPostgres(UpdatingApiBaseTest):
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
