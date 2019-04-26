# -*- coding: utf-8 -*-

from __future__ import print_function
import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy_mate.tests import engine, t_user, t_inv, User, Inventory
from sqlalchemy_mate import pt, selecting
from sqlalchemy_mate.pkg.prettytable import PrettyTable


def test_from_stmt():
    stmt = text("SELECT * FROM user LIMIT 2")
    t = pt.from_stmt(stmt, engine)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 2

    stmt = text("SELECT * FROM user WHERE user.user_id = :user_id;")
    t = pt.from_stmt(stmt, engine, user_id=1)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 1


def test_from_sql():
    sql = select([t_user])
    t = pt.from_sql(sql, engine, limit=10)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 3


def test_from_table():
    t = pt.from_table(t_inv, engine, limit=100)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 4


def test_from_object():
    Session = sessionmaker(bind=engine)
    ses = Session()

    t = pt.from_object(User, engine, limit=2)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 2

    t = pt.from_object(User, ses, limit=2)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 2

    ses.close()


def test_from_query():
    Session = sessionmaker(bind=engine)
    ses = Session()

    query = ses.query(Inventory)
    t = pt.from_query(query, engine)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 4

    query = ses.query(Inventory)
    t = pt.from_query(query, ses, limit=2)
    assert isinstance(t, PrettyTable)
    assert len(t._rows) == 2

    ses.close()


def test():
    stmt = text("SELECT * FROM user LIMIT 2")
    sql = select([t_user])
    table = t_inv
    obj = User
    Session = sessionmaker(bind=engine)
    ses = Session()
    query = ses.query(Inventory)
    resultproxy = selecting.select_all(engine, t_user)
    data = selecting.select_all(engine, t_user).fetchall()

    everything = [
        stmt,
        sql,
        table,
        obj,
        query,
        resultproxy,
        data,
    ]
    for thing in everything:
        t = pt.from_everything(thing, engine, limit=10)
        assert isinstance(t, PrettyTable)
        print(t)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
