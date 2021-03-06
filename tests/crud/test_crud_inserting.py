# -*- coding: utf-8 -*-

import random
import time

import pytest
from sqlalchemy.exc import IntegrityError

from sqlalchemy_mate.crud.inserting import smart_insert
from sqlalchemy_mate.crud.selecting import count_row
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, t_smart_insert, BaseClassForTest
)


class InsertingApiBaseTest(BaseClassForTest):
    def test_smart_insert(self):
        """
        Test performance of smart insert.

        **中文文档**

        测试smart_insert的基本功能, 以及与普通的insert比较性能。
        """
        ins = t_smart_insert.insert()

        n = 10000
        scale = 10
        n_exist = scale
        n_all = scale ** 3

        exist_id_list = [random.randint(1, n_all) for _ in range(n_exist)]
        exist_id_list = list(set(exist_id_list))
        exist_id_list.sort()

        # Smart Insert
        engine_sqlite.execute(t_smart_insert.delete())

        exist_data = [{"id": id} for id in exist_id_list]
        all_data = [{"id": i} for i in range(1, 1 + n_all)]

        engine_sqlite.execute(ins, exist_data)
        assert count_row(engine_sqlite, t_smart_insert) == len(exist_id_list)

        st = time.process_time()
        smart_insert(engine_sqlite, t_smart_insert, all_data, 5)
        elapse1 = time.process_time() - st

        assert count_row(engine_sqlite, t_smart_insert) == n_all

        # Regular Insert
        engine_sqlite.execute(t_smart_insert.delete())

        engine_sqlite.execute(ins, exist_data)
        assert count_row(engine_sqlite, t_smart_insert) == len(exist_id_list)

        st = time.process_time()
        for row in all_data:
            try:
                engine_sqlite.execute(ins, row)
            except IntegrityError:
                pass
        elapse2 = time.process_time() - st

        assert count_row(engine_sqlite, t_smart_insert) == n_all

        assert elapse1 < elapse2

    def test_smart_insert_single_row(self):
        engine_sqlite.execute(t_smart_insert.delete())
        assert count_row(engine_sqlite, t_smart_insert) == 0

        data = {"id": 1}

        smart_insert(engine_sqlite, t_smart_insert, data)
        assert count_row(engine_sqlite, t_smart_insert) == 1

        smart_insert(engine_sqlite, t_smart_insert, data)
        assert count_row(engine_sqlite, t_smart_insert) == 1


class TestInsertingApiSqlite(InsertingApiBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(IS_WINDOWS,
                    reason="no psql service container for windows")
class TestInsertingApiPostgres(InsertingApiBaseTest):
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
