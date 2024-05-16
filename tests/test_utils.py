# -*- coding: utf-8 -*-


import pytest
from sqlalchemy_mate import utils
from sqlalchemy_mate.api import EngineCreator, TimeoutError
from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    BaseCrudTest,
)


def test_ensure_list():
    assert utils.ensure_list(1) == [
        1,
    ]
    assert utils.ensure_list([1, 2, 3]) == [1, 2, 3]
    assert utils.ensure_list((1, 2, 3)) == (1, 2, 3)


class UtilityTestBase(BaseCrudTest):
    def test_timeout_good_case(self):
        utils.test_connection(self.engine, timeout=3)


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="windows doesn't have such signal",
)
class TestUtilitySqlite(UtilityTestBase):
    engine = engine_sqlite

    def test_timeout_bad_case(self):
        with pytest.raises(TimeoutError):
            engine = EngineCreator().create_sqlite()
            utils.test_connection(engine, timeout=0.000001)


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestUtilityPostgres(UtilityTestBase):
    engine = engine_psql

    def test_timeout_bad_case(self):
        engine = EngineCreator(
            host="stampy.db.elephantsql.com",
            port=5432,
            database="diyvavwx",
            username="diyvavwx",
            password="wrongpassword",
        ).create_postgresql_pg8000()
        with pytest.raises(Exception):
            utils.test_connection(engine, timeout=10)


if __name__ == "__main__":
    from sqlalchemy_mate.tests.helper import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.utils", preview=False)
