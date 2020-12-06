# -*- coding: utf-8 -*-

import random
import time

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError

from sqlalchemy_mate import selecting
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, BaseClassForOrmTest,
    Base, User, Association, Order,
)


class BulkOperationTestBase(BaseClassForOrmTest):
    def test_smart_insert(self):
        """
        Test performance of smart insert.

        **中文文档**

        测试smart_insert的基本功能, 以及与普通的insert比较性能。
        """
        scale = 6
        n_exist = scale
        n_all = scale ** 3

        exist_id_list = [random.randint(1, n_all) for _ in range(n_exist)]
        exist_id_list = list(set(exist_id_list))
        exist_id_list.sort()

        # user smart insert
        self.eng.execute(User.__table__.delete())

        exist_data = [User(id=id) for id in exist_id_list]
        all_data = [User(id=id) for id in range(1, 1 + n_all)]

        self.ses.add_all(exist_data)
        self.ses.commit()
        assert self.ses.query(User).count() == len(exist_data)

        st = time.process_time()
        op_counter = User.smart_insert(self.ses, all_data)
        elapse1 = time.process_time() - st

        assert self.ses.query(User).count() == n_all

        assert op_counter <= (0.5 * n_all)

        # user regular insert
        self.eng.execute(User.__table__.delete())

        exist_data = [User(id=id) for id in exist_id_list]
        all_data = [User(id=id) for id in range(1, 1 + n_all)]

        self.ses.add_all(exist_data)
        self.ses.commit()
        assert self.ses.query(User).count() == len(exist_data)

        st = time.process_time()
        for user in all_data:
            try:
                self.ses.add(user)
                self.ses.commit()
            except IntegrityError:
                self.ses.rollback()
            except FlushError:
                self.ses.rollback()
        elapse2 = time.process_time() - st

        assert self.ses.query(User).count() == n_all

        assert elapse1 < elapse2

    def test_smart_insert_single_object(self):
        assert selecting.count_row(self.eng, User.__table__) == 0

        user = User(id=1)
        User.smart_insert(self.eng, user)
        assert selecting.count_row(self.eng, User.__table__) == 1

        user = User(id=1)
        User.smart_insert(self.eng, user)
        assert selecting.count_row(self.eng, User.__table__) == 1

    def test_smart_update(self):
        # single primary key column
        User.smart_insert(self.eng, [User(id=1)])
        assert User.by_pk(1, self.eng).name == None

        # update
        row_count = User.update_all(
            self.eng,
            [
                User(id=1, name="Alice"),
                User(id=2, name="Bob"),
            ]
        )
        assert row_count == 1
        result = self.ses.query(User).all()
        assert len(result) == 1  # User(Bob) not updated
        result[0].name == "Alice"

        # upsert
        row_count = User.upsert_all(
            self.eng,
            [
                User(id=1, name="Adam"),
                User(id=2, name="Bob"),
            ]
        )
        assert row_count == 2

        result = self.ses.query(User).all()
        assert len(result) == 2
        result[0].name == "Adam"
        result[1].name == "Bob"

        # multiple primary key columns
        Association.smart_insert(self.ses, Association(x_id=1, y_id=1, flag=0))
        assert Association.by_pk((1, 1), self.ses).flag == 0

        # update
        row_count = Association.update_all(
            self.ses,
            [
                Association(x_id=1, y_id=1, flag=1),
                Association(x_id=1, y_id=2, flag=2),
            ]
        )
        assert row_count == 1
        result = self.ses.query(Association).all()
        assert len(result) == 1  # User(Bob) not updated
        result[0].flag == 1

        # upsert
        row_count = Association.upsert_all(
            self.eng,
            [
                Association(x_id=1, y_id=1, flag=999),
                Association(x_id=1, y_id=2, flag=2),
            ]
        )
        assert row_count == 2

        result = self.ses.query(Association).all()
        assert len(result) == 2
        result[0].flag == 999
        result[1].flag == 2

    def test_random_sample(self):
        n_order = 100
        Order.smart_insert(self.ses, [Order(id=id) for id in range(1, n_order + 1)])
        result1 = Order.random_sample(self.ses, 3)
        assert len(result1)

        result2 = Order.random_sample(self.ses, 3)
        assert len(result2)

        assert result1[0].id != result2[0].id


class TestExtendedBaseOnSqlite(BulkOperationTestBase):  # test on sqlite
    engine = engine_sqlite
    declarative_base_class = Base


@pytest.mark.skipif(IS_WINDOWS)
class TestExtendedBaseOnPostgres(BulkOperationTestBase):  # test on postgres
    engine = engine_psql
    declarative_base_class = Base


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
