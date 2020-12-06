# -*- coding: utf-8 -*-

from __future__ import print_function

import pytest
from sqlalchemy import select, text

from sqlalchemy_mate import pt, selecting
from sqlalchemy_mate.pkg.prettytable import PrettyTable
from sqlalchemy_mate.tests import (
    engine_sqlite, engine_psql,
    t_user, t_inv, User, Association, Base,
    BaseClassForTest,
)


class PrettyTableTestBase(BaseClassForTest):
    @classmethod
    def class_level_data_setup(cls):
        cls.engine.execute(t_user.delete())
        data = [{"user_id": 1, "name": "Alice"},
                {"user_id": 2, "name": "Bob"},
                {"user_id": 3, "name": "Cathy"}]
        cls.engine.execute(t_user.insert(), data)

        cls.engine.execute(t_inv.delete())
        data = [{"store_id": 1, "item_id": 1},
                {"store_id": 1, "item_id": 2},
                {"store_id": 2, "item_id": 1},
                {"store_id": 2, "item_id": 2}]
        cls.engine.execute(t_inv.insert(), data)

        cls.engine.execute(User.__table__.delete())
        cls.session.add_all([
            User(id=1, name="Alice"),
            User(id=2, name="Bob"),
            User(id=3, name="Cathy"),
        ])
        cls.session.commit()

        cls.engine.execute(Association.__table__.delete())
        cls.session.add_all([
            Association(x_id=1, y_id=1),
            Association(x_id=1, y_id=2),
            Association(x_id=2, y_id=1),
            Association(x_id=2, y_id=2),
        ])
        cls.session.commit()

    def test_from_stmt(self):
        stmt = text(f"SELECT * FROM {t_user.name} LIMIT 2")
        t = pt.from_stmt(stmt, self.engine)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 2

        stmt = text(f"SELECT * FROM {t_user.name} WHERE {t_user.name}.user_id = :user_id;")
        t = pt.from_stmt(stmt, self.engine, user_id=1)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 1

    def test_from_sql(self):
        sql = select([t_user])
        t = pt.from_sql(sql, self.engine, limit=10)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 3

    def test_from_table(self):
        t = pt.from_table(t_inv, self.engine, limit=100)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 4

    def test_from_object(self):
        t = pt.from_object(User, self.engine, limit=2)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 2

        t = pt.from_object(User, self.session, limit=2)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 2

    def test_from_query(self):
        query = self.session.query(Association)
        t = pt.from_query(query, self.engine)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 4

        query = self.session.query(Association)
        t = pt.from_query(query, self.session, limit=2)
        assert isinstance(t, PrettyTable)
        assert len(t._rows) == 2

    def test_from_everything(self):
        stmt = text(f"SELECT * FROM {t_user.name} LIMIT 2")
        sql = select([t_user])
        table = t_inv
        obj = User
        query = self.ses.query(Association)
        resultproxy = selecting.select_all(self.engine, t_user)
        data = selecting.select_all(self.engine, t_user).fetchall()

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
            t = pt.from_everything(thing, self.engine, limit=10)
            assert isinstance(t, PrettyTable)


class TestPrettyyTableSqlite(PrettyTableTestBase):
    engine = engine_sqlite
    declarative_base_class = Base


class TestPrettyyTablePostgres(PrettyTableTestBase):
    engine = engine_psql
    declarative_base_class = Base


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
