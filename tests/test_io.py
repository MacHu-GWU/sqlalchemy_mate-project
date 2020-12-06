# -*- coding: utf-8 -*-

import pytest

from sqlalchemy_mate import io
from sqlalchemy_mate.tests import (
    engine_sqlite, engine_psql, t_user, t_inv, BaseClassForTest,
)


def teardown_module(module):
    import os

    try:
        filepath = __file__.replace("test_io.py", "t_user.csv")
        os.remove(filepath)
    except:
        pass


class DataIOTestBase(BaseClassForTest):
    @classmethod
    def class_level_data_setup(cls):
        cls.engine.execute(t_user.delete())
        data = [{"user_id": 1, "name": "Alice"},
                {"user_id": 2, "name": "Bob"},
                {"user_id": 3, "name": "Cathy"}]
        cls.engine.execute(t_user.insert(), data)

    def test_table_to_csv(self):
        filepath = __file__.replace("test_io.py", "t_user.csv")
        io.table_to_csv(t_user, self.engine, filepath, chunksize=1, overwrite=True)


class TestDataIOSqlite(DataIOTestBase):
    engine = engine_sqlite


class TestDataIOPostgres(DataIOTestBase):
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
