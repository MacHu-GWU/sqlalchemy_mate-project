# -*- coding: utf-8 -*-

"""
Testing for many object DB action.
"""

import time
import random

import pytest

import sqlalchemy.orm as orm
from sqlalchemy.orm.exc import FlushError
from sqlalchemy.exc import IntegrityError

from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    BaseCrudTest,
    User,
    Association,
    Order,
)


class BulkOperationTestBase(BaseCrudTest):
    def method_level_data_teardown(self):
        self.delete_all_data_in_orm_table()

    def test_smart_insert(self):
        """
        Test performance of smart insert.

        **中文文档**

        测试smart_insert的基本功能, 以及与普通的insert比较性能。
        """
        # ------ Before State ------
        scale = 10
        n_exist = scale
        n_all = scale**3

        exist_id_list = [random.randint(1, n_all) for _ in range(n_exist)]
        exist_id_list = list(set(exist_id_list))
        exist_id_list.sort()
        n_exist = len(exist_id_list)

        # user smart insert
        exist_data = [User(id=user_id) for user_id in exist_id_list]
        all_data = [User(id=user_id) for user_id in range(1, 1 + n_all)]

        with orm.Session(self.engine) as ses:
            ses.add_all(exist_data)
            ses.commit()
            assert User.count_all(ses) == n_exist

        # ------ Invoke ------
        st = time.process_time()
        with orm.Session(self.engine) as ses:
            op_counter, insert_counter = User.smart_insert(ses, all_data)
            elapse1 = time.process_time() - st
            assert User.count_all(ses) == n_all

        # ------ After State ------
        assert op_counter <= (0.5 * n_all)
        assert insert_counter == (n_all - n_exist)

        # user regular insert
        # ------ Before State ------
        with self.eng.connect() as conn:
            conn.execute(User.__table__.delete())
            conn.commit()

        exist_data = [User(id=id) for id in exist_id_list]
        all_data = [User(id=id) for id in range(1, 1 + n_all)]

        with orm.Session(self.engine) as ses:
            ses.add_all(exist_data)
            ses.commit()
            assert User.count_all(ses) == n_exist

        st = time.process_time()
        with orm.Session(self.engine) as ses:
            for user in all_data:
                try:
                    ses.add(user)
                    ses.commit()
                except IntegrityError:
                    ses.rollback()
                except FlushError:
                    ses.rollback()
            elapse2 = time.process_time() - st

            assert User.count_all(ses) == n_all

        assert elapse1 < elapse2

    def test_smart_insert_single_object(self):
        assert User.count_all(self.eng) == 0

        user = User(id=1)
        User.smart_insert(self.eng, user)
        assert User.count_all(self.eng) == 1

        user = User(id=1)
        User.smart_insert(self.eng, user)
        assert User.count_all(self.eng) == 1

    def test_smart_update(self):
        # single primary key column
        # ------ Before State ------
        User.smart_insert(self.eng, [User(id=1)])
        with orm.Session(self.eng) as ses:
            assert ses.get(User, 1).name == None

        # ------ Invoke ------
        # update
        update_count, insert_count = User.update_all(
            self.eng,
            [
                User(id=1, name="Alice"),  # this is update
                User(id=2, name="Bob"),  # this is not insert
            ],
        )

        # ------ After State ------
        assert update_count == 1
        assert insert_count == 0

        with orm.Session(self.eng) as ses:
            assert User.count_all(ses) == 1  # User(Bob) not inserted
            assert ses.get(User, 1).name == "Alice"
            assert ses.get(User, 2) == None

        # ------ Invoke ------
        # upsert
        with orm.Session(self.eng) as ses:
            update_count, insert_count = User.upsert_all(
                ses,
                [
                    User(id=1, name="Adam"),
                    User(id=2, name="Bob"),
                ],
            )

        # ------ After State ------
        assert update_count == 1
        assert insert_count == 1

        with orm.Session(self.eng) as ses:
            assert User.count_all(ses) == 2  # User(Bob) got inserted
            assert ses.get(User, 1).name == "Adam"
            assert ses.get(User, 2).name == "Bob"

        # multiple primary key columns
        # ------ Before State ------
        Association.smart_insert(self.eng, Association(x_id=1, y_id=1, flag=0))
        assert Association.by_pk(self.eng, (1, 1)).flag == 0

        # ------ Invoke ------
        # update
        with orm.Session(self.eng) as ses:
            update_counter, insert_counter = Association.update_all(
                ses,
                [
                    Association(x_id=1, y_id=1, flag=1),  # this is update
                    Association(x_id=1, y_id=2, flag=2),  # this is not insert
                ],
            )

        # ------ After State ------
        assert update_counter == 1
        assert insert_counter == 0

        with orm.Session(self.eng) as ses:
            assert Association.count_all(ses) == 1
            assert ses.get(Association, (1, 1)).flag == 1
            assert ses.get(Association, (1, 2)) is None

        # ------ Invoke ------
        # upsert
        with orm.Session(self.eng) as ses:
            update_count, insert_count = Association.upsert_all(
                ses,
                [
                    Association(x_id=1, y_id=1, flag=999),
                    Association(x_id=1, y_id=2, flag=2),
                ],
            )

        # ------ After State ------
        assert update_count == 1
        assert insert_count == 1
        with orm.Session(self.eng) as ses:
            assert Association.count_all(ses) == 2
            assert ses.get(Association, (1, 1)).flag == 999
            assert ses.get(Association, (1, 2)).flag == 2

    def test_select_all(self):
        with orm.Session(self.eng) as ses:
            ses.add_all(
                [
                    User(id=1),
                    User(id=2),
                    User(id=3),
                ]
            )
            ses.commit()

            user_list = User.select_all(self.eng)
            assert len(user_list) == 3
            assert isinstance(user_list[0], User)

            user_list = User.select_all(ses)
            assert len(user_list) == 3
            assert isinstance(user_list[0], User)

    def test_random_sample(self):
        n_order = 1000
        Order.smart_insert(self.eng, [Order(id=id) for id in range(1, n_order + 1)])

        result1 = Order.random_sample(self.eng, limit=5)
        assert len(result1) == 5

        with orm.Session(self.eng) as ses:
            result2 = Order.random_sample(ses, limit=5)
            assert len(result2) == 5

        assert sum([od1.id != od2.id for od1, od2 in zip(result1, result2)]) >= 1

        self.psql_only_test_case()

    def psql_only_test_case(self):
        result3 = Order.random_sample(self.eng, perc=10)

        with orm.Session(self.eng) as ses:
            result4 = Order.random_sample(ses, perc=10)

        assert result3[0].id != result4[0].id


class TestExtendedBaseOnSqlite(BulkOperationTestBase):  # test on sqlite
    engine = engine_sqlite

    def psql_only_test_case(self):
        pass


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestExtendedBaseOnPostgres(BulkOperationTestBase):  # test on postgres
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_mate.orm.extended_declarative_base",
        preview=False,
    )
