# -*- coding: utf-8 -*-

import pytest

from sqlalchemy_mate import io
from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    t_user,
    BaseCrudTest,
)


def teardown_module(module):
    import os

    try:
        filepath = __file__.replace("test_io.py", "t_user.csv")
        os.remove(filepath)
    except:
        pass


class DataIOTestBase(BaseCrudTest):
    @classmethod
    def class_level_data_setup(cls):
        with cls.engine.connect() as connection:
            connection.execute(t_user.delete())
            data = [
                {"user_id": 1, "name": "Alice"},
                {"user_id": 2, "name": "Bob"},
                {"user_id": 3, "name": "Cathy"},
            ]
            connection.execute(t_user.insert(), data)

    def test_table_to_csv(self):
        filepath = __file__.replace("test_io.py", "t_user.csv")
        io.table_to_csv(t_user, self.engine, filepath, chunksize=1, overwrite=True)


class TestDataIOSqlite(DataIOTestBase):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestDataIOPostgres(DataIOTestBase):
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests.helper import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.io", preview=False)
