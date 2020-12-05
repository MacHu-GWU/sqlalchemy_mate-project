# -*- coding: utf-8 -*-

"""
Testing for single object DB action.
"""

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy import select, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy_mate import engine_creator
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
    def test_by_id(self):
        engine.execute(User.__table__.delete())
        user = User(id=1, name="Michael Jackson")
        User.smart_insert(engine, user)

        assert User.by_pk(1, engine).name == "Michael Jackson"
        assert User.by_pk(100, self.ses) == None

        engine.execute(Association.__table__.delete())
        asso = Association(x_id=1, y_id=2, flag=999)
        Association.smart_insert(engine, asso)

        assert Association.by_pk((1, 2), engine).flag == 999
        assert Association.by_pk((1, 2), self.ses).flag == 999
        assert Association.by_pk(dict(x_id=1, y_id=2), self.ses).flag == 999

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


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
