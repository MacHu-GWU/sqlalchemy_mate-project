# -*- coding: utf-8 -*-

import pytest

import sqlalchemy as sa
import sqlalchemy.orm as orm

from prettytable import PrettyTable

from sqlalchemy_mate.api import pt, selecting
from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    t_user,
    t_inv,
    User,
    Association,
    BaseCrudTest,
)


class PrettyTableTestBase(BaseCrudTest):
    @classmethod
    def class_level_data_setup(cls):
        cls.delete_all_data_in_core_table()
        cls.delete_all_data_in_orm_table()

        t_user_data = [
            {"user_id": 1, "name": "Alice"},
            {"user_id": 2, "name": "Bob"},
            {"user_id": 3, "name": "Cathy"},
            {"user_id": 4, "name": "David"},
        ]

        t_inv_data = [
            {"store_id": 1, "item_id": 1},
            {"store_id": 1, "item_id": 2},
            {"store_id": 2, "item_id": 1},
            {"store_id": 2, "item_id": 2},
            {"store_id": 3, "item_id": 1},
            {"store_id": 3, "item_id": 2},
        ]

        with cls.engine.connect() as connection:
            connection.execute(sa.insert(t_user), t_user_data)
            connection.execute(t_inv.insert(), t_inv_data)
            connection.commit()

        with orm.Session(cls.engine) as ses:
            ses.add_all(
                [
                    User(id=1, name="X"),
                    User(id=2, name="Y"),
                    User(id=3, name="Z"),
                ]
            )
            ses.add_all(
                [
                    Association(x_id=1, y_id=1, flag=1),
                    Association(x_id=1, y_id=2, flag=2),
                    Association(x_id=2, y_id=1, flag=3),
                    Association(x_id=2, y_id=2, flag=4),
                ]
            )
            ses.commit()

    def test_get_headers(self):
        with orm.Session(self.eng) as ses:
            user = ses.get(User, 1)
            keys, values = pt.get_keys_values(user)
            assert keys == ["id", "name"]
            assert values == [1, "X"]

        with self.eng.connect() as connection:
            row = connection.execute(sa.select(t_user).limit(1)).fetchone()
            keys, values = pt.get_keys_values(row)
            assert keys == ["user_id", "name"]
            assert values == [1, "Alice"]

    def test_from_result(self):
        with orm.Session(self.eng) as ses:
            res = ses.scalars(sa.select(User))
            ptable = pt.from_result(res)
            assert len(ptable._rows) == 3

        with self.eng.connect() as connection:
            res = connection.execute(sa.select(t_user))
            ptable = pt.from_result(res)
            assert len(ptable._rows) == 4

    def test_from_text_clause(self):
        stmt = sa.text(f"SELECT * FROM {t_user.name} LIMIT 2")
        tb = pt.from_text_clause(stmt, self.eng)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 2
        stmt = sa.text(
            f"SELECT * FROM {t_user.name} WHERE {t_user.name}.user_id = :user_id;"
        )
        stmt = stmt.bindparams(user_id=1)
        tb = pt.from_text_clause(stmt, self.eng)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 1

    def test_from_stmt(self):
        stmt = sa.select(t_inv)
        tb = pt.from_stmt(stmt, self.eng)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 6

        stmt = sa.select(t_inv).limit(3)
        tb = pt.from_stmt(stmt, self.eng)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 3

        # ORM also works
        query = sa.select(User).where(User.id >= 2)
        tb = pt.from_stmt(query, self.eng)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 2

        query = sa.select(User).limit(2)
        tb = pt.from_stmt(query, self.eng)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 2

    def test_from_table(self):
        tb = pt.from_table(t_inv, self.eng, limit=10)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 6

        tb = pt.from_table(t_inv, self.eng, limit=3)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 3

    def test_from_model(self):
        tb = pt.from_model(Association, self.eng, limit=2)
        assert isinstance(tb, PrettyTable)
        assert len(tb._rows) == 2

        with orm.Session(self.eng) as ses:
            tb = pt.from_model(User, ses, limit=2)
            assert isinstance(tb, PrettyTable)
            assert len(tb._rows) == 2

    def test_from_data(self):
        with self.eng.connect() as connection:
            rows = [row._asdict() for row in connection.execute(sa.select(t_user))]
            tb = pt.from_dict_list(rows)
            assert isinstance(tb, PrettyTable)
            assert len(tb._rows) == 4

    def test_from_everything(self):
        t = sa.text(f"SELECT * FROM {t_user.name} LIMIT 2")
        stmt = sa.select(t_user)
        table = t_inv
        query = sa.select(Association)
        orm_class = User
        result = selecting.select_all(self.engine, t_user)
        data = [row._asdict() for row in selecting.select_all(self.engine, t_user)]

        everything = [
            t,
            stmt,
            table,
            query,
            orm_class,
            result,
            data,
            f"SELECT user_id FROM {t_user.name} WHERE name = 'Bob'",
        ]
        for thing in everything:
            tb = pt.from_everything(thing, self.engine, limit=10)
            assert isinstance(tb, PrettyTable)

        with pytest.raises(Exception):
            pt.from_everything(None)


class TestPrettyTableSqlite(PrettyTableTestBase):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPrettyTablePostgres(PrettyTableTestBase):
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests.helper import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.pt", preview=False)
