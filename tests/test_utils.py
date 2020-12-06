# -*- coding: utf-8 -*-

import sys

import pytest

from sqlalchemy_mate import utils
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, User, Base, BaseClassForTest,
)


def test_ensure_list():
    assert utils.ensure_list(1) == [1, ]
    assert utils.ensure_list([1, 2, 3]) == [1, 2, 3]
    assert utils.ensure_list((1, 2, 3)) == (1, 2, 3)


def test_timeout():
    from sqlalchemy_mate import EngineCreator, TimeoutError

    if sys.platform.lower().startswith("win"):
        return

    engine = EngineCreator(
        host="stampy.db.elephantsql.com", port=5432,
        database="diyvavwx", username="diyvavwx", password="wrongpassword"
    ).create_postgresql_psycopg2()
    with pytest.raises(Exception):
        utils.test_connection(engine, timeout=10)

    engine = EngineCreator().create_sqlite()
    with pytest.raises(TimeoutError):
        utils.test_connection(engine, timeout=0.000001)
    utils.test_connection(engine, timeout=3)


class UtilityTestBase(BaseClassForTest):
    @classmethod
    def class_level_data_setup(cls):
        cls.engine.execute(User.__table__.delete())
        cls.session.add_all([
            User(id=1, name="Alice"),
            User(id=2, name="Bob"),
            User(id=3, name="Cathy"),
        ])
        cls.session.commit()

    def test_convert_query_to_sql_statement(self):
        query = self.session.query(User)
        sql = utils.convert_query_to_sql_statement(query)
        assert len(self.engine.execute(sql).fetchall()) == 3
        self.session.close()


class TestUtilitySqlite(UtilityTestBase):
    engine = engine_sqlite
    declarative_base_class = Base


@pytest.mark.skipif(IS_WINDOWS)
class TestUtilityPostgres(UtilityTestBase):
    engine = engine_psql
    declarative_base_class = Base


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
