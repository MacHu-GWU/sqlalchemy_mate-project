# -*- coding: utf-8 -*-

import random
import time

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import FlushError

from sqlalchemy_mate import engine_creator, selecting
from sqlalchemy_mate.orm.extended_declarative_base import ExtendedBase

Base = declarative_base()
engine = engine_creator.create_sqlite()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_user"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


class Association(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_associattion"

    x_id = Column(Integer, primary_key=True)
    y_id = Column(Integer, primary_key=True)
    flag = Column(Integer)


class Order(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_order"

    id = Column(Integer, primary_key=True)


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)


class TestBase(object):
    ses = None

    @classmethod
    def setup_class(cls):
        cls.ses = Session()

    @classmethod
    def teardown_class(cls):
        cls.ses.close()


class TestExtendedBase(TestBase):
    def test_smart_insert(self):
        """
        Test performance of smart insert.

        **中文文档**

        测试smart_insert的基本功能, 以及与普通的insert比较性能。
        """
        Session = sessionmaker(bind=engine)
        ses = Session()

        scale = 6
        n_exist = scale
        n_all = scale ** 3

        exist_id_list = [random.randint(1, n_all) for _ in range(n_exist)]
        exist_id_list = list(set(exist_id_list))
        exist_id_list.sort()

        # user smart insert
        engine.execute(User.__table__.delete())

        exist_data = [User(id=id) for id in exist_id_list]
        all_data = [User(id=id) for id in range(1, 1 + n_all)]

        ses.add_all(exist_data)
        ses.commit()
        assert ses.query(User).count() == len(exist_data)

        st = time.process_time()
        op_counter = User.smart_insert(ses, all_data)
        elapse1 = time.process_time() - st

        assert ses.query(User).count() == n_all

        assert op_counter <= (0.5 * n_all)

        # user regular insert
        engine.execute(User.__table__.delete())

        exist_data = [User(id=id) for id in exist_id_list]
        all_data = [User(id=id) for id in range(1, 1 + n_all)]

        ses.add_all(exist_data)
        ses.commit()
        assert ses.query(User).count() == len(exist_data)

        st = time.process_time()
        for user in all_data:
            try:
                ses.add(user)
                ses.commit()
            except IntegrityError:
                ses.rollback()
            except FlushError:
                ses.rollback()
        elapse2 = time.process_time() - st

        assert ses.query(User).count() == n_all

        assert elapse1 < elapse2

        ses.close()

    def test_smart_insert_single_object(self):
        engine.execute(User.__table__.delete())
        assert selecting.count_row(engine, User.__table__) == 0

        user = User(id=1)
        User.smart_insert(engine, user)
        assert selecting.count_row(engine, User.__table__) == 1

        user = User(id=1)
        User.smart_insert(engine, user)
        assert selecting.count_row(engine, User.__table__) == 1

    def test_smart_update(self):
        # single primary key column
        engine.execute(User.__table__.delete())
        User.smart_insert(engine, [User(id=1)])
        assert User.by_pk(1, engine).name == None

        # update
        row_count = User.update_all(
            engine,
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
            engine,
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
        engine.execute(Association.__table__.delete())
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
            engine,
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


class TestOrder(TestBase):
    def test(self):
        n_order = 100

        Order.smart_insert(self.ses, [Order(id=id) for id in range(1, n_order + 1)])
        result1 = Order.random(self.ses, 3)
        assert len(result1)

        result2 = Order.random(self.ses, 3)
        assert len(result2)

        assert result1[0].id != result2[0].id


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
