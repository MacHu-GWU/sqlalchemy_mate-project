# -*- coding: utf-8 -*-

import random
import time

import pytest
from sqlalchemy.exc import IntegrityError

from sqlalchemy_mate.crud.inserting import smart_insert
from sqlalchemy_mate.crud.selecting import count_row
from sqlalchemy_mate.crud.deleting import delete_all
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    t_smart_insert,
    BaseTest,
)


class InsertingApiBaseTest(BaseTest):
    def teardown_method(self, method):
        """
        Make sure data in all table is cleared after each test cases.
        """
        self.delete_all_data_in_core_table()

    def test_smart_insert(self):
        """
        Test performance of smart insert.

        **中文文档**

        测试smart_insert的基本功能, 以及与普通的insert比较性能。
        """
        # Use Smart Insert Method
        # ------ Before State ------
        scale = 10  # 测试数据的数量级, 总数据量是已有的数据量的立方, 建议 5 ~ 10
        n_exist = scale
        n_all = scale**3

        exist_id_list = [random.randint(1, n_all) for _ in range(n_exist)]
        exist_id_list = list(set(exist_id_list))
        exist_id_list.sort()
        n_exist = len(exist_id_list)

        # Smart Insert
        exist_data = [{"id": id} for id in exist_id_list]
        all_data = [{"id": i} for i in range(1, 1 + n_all)]

        op_count, ins_count = smart_insert(self.engine, t_smart_insert, exist_data, 5)
        assert op_count == 1
        assert ins_count == n_exist
        assert count_row(self.engine, t_smart_insert) == n_exist

        # ------ Invoke ------
        st = time.process_time()
        op_count, ins_count = smart_insert(self.engine, t_smart_insert, all_data, 5)
        assert op_count <= (0.5 * n_all)
        assert ins_count == (n_all - n_exist)
        elapse1 = time.process_time() - st

        # ------ After State ------
        assert count_row(self.engine, t_smart_insert) == n_all

        # Use Regular Insert Method
        # ------ Before State ------
        with self.engine.connect() as connection:
            connection.execute(t_smart_insert.delete())
            connection.commit()

        ins = t_smart_insert.insert()
        with self.engine.connect() as connection:
            connection.execute(ins, exist_data)
            connection.commit()

        assert count_row(self.engine, t_smart_insert) == n_exist

        # ------ Invoke ------
        st = time.process_time()
        with self.engine.connect() as connection:
            for row in all_data:
                try:
                    connection.execute(ins, row)
                    connection.commit()
                except IntegrityError:
                    connection.rollback()
        elapse2 = time.process_time() - st

        assert count_row(self.engine, t_smart_insert) == n_all

        # ------ After State ------
        assert elapse1 < elapse2

    def _test_smart_insert_single_row(self):
        assert count_row(self.engine, t_smart_insert) == 0

        data = {"id": 1}
        op_count, ins_count = smart_insert(self.engine, t_smart_insert, data)
        assert op_count == 1
        assert ins_count == 1
        assert count_row(self.engine, t_smart_insert) == 1

        op_count, ins_count = smart_insert(self.engine, t_smart_insert, data)
        assert op_count == 0
        assert ins_count == 0
        assert count_row(self.engine, t_smart_insert) == 1


class TestInsertingApiSqlite(InsertingApiBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestInsertingApiPostgres(InsertingApiBaseTest):
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.crud.inserting", preview=False)
