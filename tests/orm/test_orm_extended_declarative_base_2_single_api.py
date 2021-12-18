# -*- coding: utf-8 -*-

"""
Testing for single object DB action.
"""

import pytest

import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, BaseTest,
    User, Association, Order,
)


class SingleOperationBaseTest(BaseTest):
    def method_level_data_setup(self):
        self.delete_all_data_in_orm_table()

    def method_level_data_teardown(self):
        self.delete_all_data_in_orm_table()

    def test_by_pk(self):
        with Session(self.eng) as ses:
            ses.add(User(id=1, name="alice"))
            ses.commit()

        assert User.by_pk(self.eng, 1).name == "alice"
        assert User.by_pk(self.eng, (1,)).name == "alice"
        assert User.by_pk(self.eng, [1, ]).name == "alice"
        assert User.by_pk(self.eng, 0) is None
        with Session(self.eng) as ses:
            assert User.by_pk(ses, 1).name == "alice"
            assert User.by_pk(ses, (1,)).name == "alice"
            assert User.by_pk(ses, [1, ]).name == "alice"
            assert User.by_pk(ses, 0) is None

        with Session(self.eng) as ses:
            ses.add(Association(x_id=1, y_id=2, flag=999))
            ses.commit()

        assert Association.by_pk(self.eng, (1, 2)).flag == 999
        assert Association.by_pk(self.eng, [1, 2]).flag == 999
        assert Association.by_pk(self.eng, (0, 0)) is None
        with Session(self.eng) as ses:
            assert Association.by_pk(ses, (1, 2)).flag == 999
            assert Association.by_pk(ses, [1, 2]).flag == 999
            assert Association.by_pk(ses, [0, 0]) is None

    def test_by_sql(self):
        assert User.count_all(self.eng) == 0
        with Session(self.eng) as ses:
            user_list = [
                User(id=1, name="mr x"),
                User(id=2, name="mr y"),
                User(id=3, name="mr z"),
            ]
            ses.add_all(user_list)
            ses.commit()
        assert User.count_all(self.eng) == 3

        expected = ["mr y", "mr z"]

        results = User.by_sql(
            self.eng,
            """
            SELECT * 
            FROM extended_declarative_base_user t
            WHERE t.id >= 2
            """,
        )
        assert [user.name for user in results] == expected

        results = User.by_sql(
            self.eng,
            sa.text("""
            SELECT * 
            FROM extended_declarative_base_user t
            WHERE t.id >= 2
            """),
        )
        assert [user.name for user in results] == expected


class TestExtendedBaseOnSqlite(SingleOperationBaseTest):  # test on sqlite
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestExtendedBaseOnPostgres(SingleOperationBaseTest):  # test on postgres
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
