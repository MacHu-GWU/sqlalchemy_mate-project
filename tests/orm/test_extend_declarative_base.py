# -*- coding: utf-8 -*-

import pytest
from pytest import raises

import time
import random
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, text
from collections import OrderedDict

from sqlalchemy_mate.orm.extended_declarative_base import ExtendedBase
from sqlalchemy_mate import engine_creator, selecting

Base = declarative_base()
engine = engine_creator.create_sqlite()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_user"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


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
    def test_keys(self):
        assert User.keys() == ["id", "name"]
        assert User(id=1).keys() == ["id", "name"]

    def test_values(self):
        assert User(id=1, name="Alice").values() == [1, "Alice"]
        assert User(id=1).values() == [1, None]

    def test_items(self):
        assert User(id=1, name="Alice").items() == [
            ("id", 1), ("name", "Alice")]
        assert User(id=1).items() == [("id", 1), ("name", None)]

    def test_repr(self):
        user = User(id=1, name="Alice")
        assert str(user) == "User(id=1, name='Alice')"

    def test_to_dict(self):
        assert User(id=1, name="Alice").to_dict() == {"id": 1, "name": "Alice"}
        assert User(id=1).to_dict() == {"id": 1, "name": None}
        assert User(id=1).to_dict(include_null=False) == {"id": 1}

    def test_to_OrderedDict(self):
        assert User(id=1, name="Alice").to_OrderedDict(include_null=True) == \
               OrderedDict([
                   ("id", 1), ("name", "Alice"),
               ])

        assert User(id=1).to_OrderedDict(include_null=True) == \
               OrderedDict([
                   ("id", 1), ("name", None),
               ])
        assert User(id=1).to_OrderedDict(include_null=False) == \
               OrderedDict([
                   ("id", 1),
               ])

        assert User(name="Alice").to_OrderedDict(include_null=True) == \
               OrderedDict([
                   ("id", None), ("name", "Alice"),
               ])
        assert User(name="Alice").to_OrderedDict(include_null=False) == \
               OrderedDict([
                   ("name", "Alice"),
               ])

    def test_glance(self):
        user = User(id=1, name="Alice")
        # user.glance()

    def test_absorb(self):
        user1 = User(id=1)
        user2 = User(name="Alice")
        user3 = User(name="Bob")

        user1.absorb(user2)
        assert user1.to_dict() == {"id": 1, "name": "Alice"}
        user1.absorb(user3)
        assert user1.to_dict() == {"id": 1, "name": "Bob"}

        with raises(TypeError):
            user1.absorb({"name": "Alice"})

    def test_revise(self):
        user = User(id=1)
        user.revise({"name": "Alice"})
        assert user.to_dict() == {"id": 1, "name": "Alice"}

        user = User(id=1, name="Alice")
        user.revise({"name": "Bob"})
        assert user.to_dict() == {"id": 1, "name": "Bob"}

        with raises(TypeError):
            user.revise(User(name="Bob"))

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

    def test_primary_key_and_id_field(self):
        assert User.pk_names() == ("id",)
        assert User.id_field_name() == "id"
        assert User.pk_names() == ("id",)
        assert User.id_field_name() == "id"

        assert User(id=1).pk_values() == (1,)
        assert User(id=1).id_field_value() == 1

    def test_by_id(self):
        engine.execute(User.__table__.delete())
        user = User(id=1, name="Michael Jackson")
        User.smart_insert(engine, user)
        assert User.by_id(1, engine).name == "Michael Jackson"
        assert User.by_id(100, engine) == None

    def test_by_sql(self):
        engine.execute(User.__table__.delete())
        User.smart_insert(engine, [
            User(id=1, name="Alice"),
            User(id=2, name="Bob"),
            User(id=3, name="Cathy"),
        ])
        t_user = User.__table__

        sql = select([t_user]).where(t_user.c.id >= 2)
        assert len(User.by_sql(sql, engine)) == 2

        sql = select([t_user]).where(t_user.c.id >= 2).limit(1)
        assert len(User.by_sql(sql, engine)) == 1

        sql = text("""
        SELECT *
        FROM extended_declarative_base_user as user
        WHERE user.id >= 2
        """)
        assert len(User.by_sql(sql, engine)) == 2

    def test_smart_update(self):
        ses = Session()

        engine.execute(User.__table__.delete())
        User.smart_insert(engine, [User(id=1)])

        User.update_all(
            engine, [User(id=1, name="Alice"), User(id=2, name="Bob")])
        result = ses.query(User).all()
        assert len(result) == 1
        result[0].name == "Alice"

        User.upsert_all(
            engine, [User(id=1, name="Adam"), User(id=2, name="Bob")])
        result = ses.query(User).all()
        assert len(result) == 2
        result[0].name == "Adam"
        result[1].name == "Bob"

        ses.close()


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
