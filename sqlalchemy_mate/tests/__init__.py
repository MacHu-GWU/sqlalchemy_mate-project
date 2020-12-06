# -*- coding: utf-8 -*-

import sys
import typing

from sqlalchemy import String, Integer
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker, Session

from ..engine_creator import EngineCreator
from ..orm.extended_declarative_base import ExtendedBase

IS_WINDOWS = sys.platform.lower().startswith("win")

# use make run-psql to run postgres container on local
engine_sqlite = create_engine("sqlite:///:memory:")

engine_psql = EngineCreator(
    username="postgres",
    password="password",
    database="postgres",
    host="localhost",
    port=43347,
).create_postgresql_psycopg2()

metadata = MetaData()

t_user = Table(
    "t_user", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("name", String),
)

t_inv = Table(
    "t_inventory", metadata,
    Column("store_id", Integer, primary_key=True),
    Column("item_id", Integer, primary_key=True),
)

t_smart_insert = Table(
    "t_smart_insert", metadata,
    Column("id", Integer, primary_key=True),
)

t_cache = Table(
    "t_cache", metadata,
    Column("key", String(), primary_key=True),
    Column("value", Integer()),
)

t_graph = Table(
    "t_edge", metadata,
    Column("x_node_id", Integer, primary_key=True),
    Column("y_node_id", Integer, primary_key=True),
    Column("value", Integer),
)


# --- Orm
Base = declarative_base()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_user"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


class Association(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_association"

    x_id = Column(Integer, primary_key=True)
    y_id = Column(Integer, primary_key=True)
    flag = Column(Integer)


class Order(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_order"

    id = Column(Integer, primary_key=True)


class BankAccount(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_bank_account"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    pin = Column(String)


class PostTagAssociation(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_post_tag_association"

    post_id = Column(Integer, primary_key=True)
    tag_id = Column(Integer, primary_key=True)
    description = Column(String)


class BaseClassForTest:
    engine = None  # type: Engine
    session = None  # type: Session
    declarative_base_class = None  # type: typing.Type[Base]
    session_class = None  # type: typing.Type[Session]

    @classmethod
    def class_level_data_setup(cls):
        pass

    def method_level_data_setup(self):
        pass

    @classmethod
    def setup_class(cls):  # it is called before all test method invocation
        if cls.engine is not None:
            metadata.create_all(cls.engine)
            if cls.declarative_base_class is not None:
                cls.declarative_base_class.metadata.create_all(cls.engine)
            cls.session_class = sessionmaker(bind=cls.engine)
            cls.session = cls.session_class()
            cls.class_level_data_setup()

    @classmethod
    def teardown_class(cls):  # it is called after all test method invocation
        if cls.session is not None:
            cls.session.close()

    def setup_method(self, method):  # it is called around each method invocation
        if self.engine is not None:
            self.method_level_data_setup()

    @property
    def eng(self):
        return self.engine

    @property
    def ses(self):
        return self.session


class BaseClassForOrmTest(BaseClassForTest):
    def setup_method(self, method):
        self.eng.execute(User.__table__.delete())
        self.eng.execute(Association.__table__.delete())
        self.eng.execute(Order.__table__.delete())
        self.eng.execute(BankAccount.__table__.delete())
        self.eng.execute(PostTagAssociation.__table__.delete())
