#!/usr/bin/env python
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
from collections import OrderedDict

from sqlalchemy_mate.orm.extended_declarative_base import ExtendedBase
from sqlalchemy_mate import engine_creator, selecting

Base = declarative_base()
engine = engine_creator.create_sqlite()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_user"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


Base.metadata.create_all(engine)


class TestExtendedBase(object):
    def test_keys(self):
        assert User(id=1, name="Alice").keys() == ["id", "name"]
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

        scale = 5
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

        st = time.clock()
        User.smart_insert(ses, all_data)
        elapse1 = time.clock() - st

        assert ses.query(User).count() == n_all

        # user regular insert
        engine.execute(User.__table__.delete())

        exist_data = [User(id=id) for id in exist_id_list]
        all_data = [User(id=id) for id in range(1, 1 + n_all)]

        ses.add_all(exist_data)
        ses.commit()
        assert ses.query(User).count() == len(exist_data)

        st = time.clock()
        for user in all_data:
            try:
                ses.add(user)
                ses.commit()
            except IntegrityError:
                ses.rollback()
            except FlushError:
                ses.rollback()
        elapse2 = time.clock() - st

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


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
