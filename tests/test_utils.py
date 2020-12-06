# -*- coding: utf-8 -*-

import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy_mate.tests import engine, User
from sqlalchemy_mate import utils


def test_ensure_list():
    assert utils.ensure_list(1) == [1, ]
    assert utils.ensure_list([1, 2, 3]) == [1, 2, 3]
    assert utils.ensure_list((1, 2, 3)) == (1, 2, 3)


def test_convert_query_to_sql_statement():
    ses = sessionmaker(bind=engine)()
    query = ses.query(User)
    sql = utils.convert_query_to_sql_statement(query)
    assert len(engine.execute(sql).fetchall()) == 3
    ses.close()


def test_timeout():
    from sqlalchemy_mate import EngineCreator, TimeoutError

    engine = EngineCreator(
        host="stampy.db.elephantsql.com", port=5432,
        database="diyvavwx", username="diyvavwx", password="wrongpassword"
    ).create_postgresql_psycopg2()
    with pytest.raises(Exception):
        utils.test_connection(engine, timeout=10)

    engine = EngineCreator().create_sqlite()
    with pytest.raises(TimeoutError):
        utils.test_connection(engine, timeout=0)
    utils.test_connection(engine, timeout=3)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
