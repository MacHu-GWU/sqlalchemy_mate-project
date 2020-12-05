# -*- coding: utf-8 -*-

import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

from sqlalchemy_mate.orm.extended_declarative_base import ExtendedBase
from sqlalchemy_mate import EngineCreator

Base = declarative_base()
engine = EngineCreator().create_sqlite()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_user"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    pin = Column(String)


class PostTagAssociation(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_post_tag_association"

    post_id = Column(Integer, primary_key=True)
    tag_id = Column(Integer, primary_key=True)
    description = Column(String)

    _settings_engine = engine


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


class TestExtendedBaseEdgeCase(object):
    ses = None

    @classmethod
    def setup_class(cls):
        cls.ses = Session()

    @classmethod
    def teardown_class(cls):
        cls.ses.close()

    def test_absorb(self):
        user1 = User(id=1, name="Alice", pin="1234")
        user2 = User(name="Bob")
        user1.absorb(user2)
        assert user1.values() == [1, "Bob", "1234"]

        user1 = User(id=1, name="Alice", pin="1234")
        user2 = User(name="Bob")
        user1.absorb(user2, ignore_none=False)
        assert user1.values() == [None, "Bob", None]

    def test_revise(self):
        user1 = User(id=1, name="Alice", pin="1234")
        user1.revise(dict(name="Bob"))
        assert user1.values() == [1, "Bob", "1234"]

        user1 = User(id=1, name="Alice", pin="1234")
        user1.revise(dict(name="Bob", pin=None))
        assert user1.values() == [1, "Bob", "1234"]

        user1 = User(id=1, name="Alice", pin="1234")
        user1.revise(dict(name="Bob", pin=None), ignore_none=False)
        assert user1.values() == [1, "Bob", None]

    def test_by_pk(self):
        PostTagAssociation.smart_insert(
            PostTagAssociation.get_eng(), [
                PostTagAssociation(post_id=1, tag_id=1, description="1-1"),
                PostTagAssociation(post_id=1, tag_id=2, description="1-2"),
            ]
        )
        PostTagAssociation.smart_insert(
            PostTagAssociation.get_ses(), [
                PostTagAssociation(post_id=1, tag_id=3, description="1-3"),
                PostTagAssociation(post_id=1, tag_id=4, description="1-4"),
            ]
        )

        pta = PostTagAssociation.by_pk((1, 2), self.ses)
        assert pta.post_id == 1
        assert pta.tag_id == 2

    def test_engine_and_session(self):
        PostTagAssociation.make_session()
        PostTagAssociation.close_session()
        PostTagAssociation.get_eng()
        PostTagAssociation.get_ses()
        PostTagAssociation.close_session()

if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
