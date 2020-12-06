# -*- coding: utf-8 -*-

"""
Testing for single object DB action.
"""

import pytest
from sqlalchemy import select, text

from sqlalchemy_mate.tests import (
    engine_sqlite, engine_psql, BaseClassForOrmTest,
    Base, User, Association,
)


class SingleObjectApiTestBase(BaseClassForOrmTest):
    def test_by_pk(self):
        user = User(id=1, name="Michael Jackson")
        User.smart_insert(self.eng, user)

        assert User.by_pk(1, self.eng).name == "Michael Jackson"
        assert User.by_pk(100, self.ses) == None

        asso = Association(x_id=1, y_id=2, flag=999)
        Association.smart_insert(self.eng, asso)

        assert Association.by_pk((1, 2), self.eng).flag == 999
        assert Association.by_pk((1, 2), self.ses).flag == 999
        assert Association.by_pk(dict(x_id=1, y_id=2), self.ses).flag == 999

    def test_by_sql(self):
        User.smart_insert(self.eng, [
            User(id=1, name="Alice"),
            User(id=2, name="Bob"),
            User(id=3, name="Cathy"),
        ])
        t_user = User.__table__

        sql = select([t_user]).where(t_user.c.id >= 2)
        assert len(User.by_sql(sql, self.eng)) == 2

        sql = select([t_user]).where(t_user.c.id >= 2).limit(1)
        assert len(User.by_sql(sql, self.eng)) == 1

        sql = text("""
        SELECT *
        FROM extended_declarative_base_user AS t_user
        WHERE t_user.id >= 2
        """)
        assert len(User.by_sql(sql, self.eng)) == 2


class TestExtendedBaseOnSqlite(SingleObjectApiTestBase):  # test on sqlite
    engine = engine_sqlite
    declarative_base_class = Base


class TestExtendedBaseOnPostgres(SingleObjectApiTestBase):  # test on postgres
    engine = engine_psql
    declarative_base_class = Base


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
