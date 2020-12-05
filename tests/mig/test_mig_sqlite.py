# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy_mate import EngineCreator

engine = EngineCreator.create_sqlite()
Base = declarative_base()


class User(object):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)


# Base.metadata.create_all(engine)

class UserNew(object):
    __tablename__ = "users_new"


def test_alt_table():
    pass


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
