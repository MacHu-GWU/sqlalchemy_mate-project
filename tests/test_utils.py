#!/usr/bin/env python
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
    assert engine.execute(sql.count()).fetchone()[0] == 3
    ses.close()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
