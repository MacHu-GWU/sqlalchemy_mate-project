#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy_mate.tests import engine, t_user, t_inv, User, Inventory
from sqlalchemy_mate import pt
from sqlalchemy_mate.pkg.prettytable import PrettyTable


def test():
    sql = select([t_user])
    t = pt.from_sql(sql, engine, limit=10)
    assert isinstance(t, PrettyTable)
    # print(t)

    table = t_inv
    t = pt.from_table(table, engine, limit=10)
    assert isinstance(t, PrettyTable)
    # print(t)

    obj = User
    t = pt.from_object(obj, engine, limit=10)
    assert isinstance(t, PrettyTable)
    # print(t)

    Session = sessionmaker(bind=engine)
    ses = Session()
    query = ses.query(Inventory)
    t = pt.from_query(query, engine, limit=10)
    assert isinstance(t, PrettyTable)
    # print(t)

    everything = [
        sql,
        table,
        obj,
        query,
    ]
    for thing in everything:
        t = pt.from_everything(thing, engine, limit=10)
        assert isinstance(t, PrettyTable)
        # print(t)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
