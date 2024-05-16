# -*- coding: utf-8 -*-

"""
这个模块提供了一些对 sqlalchemy_mate 中针对 core 和 orm 的功能的测试.
"""

import sqlalchemy as sa
import sqlalchemy.orm as orm

from ..orm.extended_declarative_base import ExtendedBase

metadata = sa.MetaData()

t_user = sa.Table(
    "t_user",
    metadata,
    sa.Column("user_id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String),
)

t_inv = sa.Table(
    "t_inventory",
    metadata,
    sa.Column("store_id", sa.Integer, primary_key=True),
    sa.Column("item_id", sa.Integer, primary_key=True),
)

t_smart_insert = sa.Table(
    "t_smart_insert",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
)

t_cache = sa.Table(
    "t_cache",
    metadata,
    sa.Column("key", sa.String, primary_key=True),
    sa.Column("value", sa.Integer),
)

t_graph = sa.Table(
    "t_edge",
    metadata,
    sa.Column("x_node_id", sa.Integer, primary_key=True),
    sa.Column("y_node_id", sa.Integer, primary_key=True),
    sa.Column("value", sa.Integer),
)

# --- Orm
Base = orm.declarative_base()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_user"

    _settings_major_attrs = ["id", "name"]

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    # name: orm.Mapped[str] = orm.mapped_column(sa.String, unique=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String, nullable=True)


class Association(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_association"

    x_id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    y_id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    flag: orm.Mapped[int] = orm.mapped_column(sa.Integer)


class Order(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_order"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)


class BankAccount(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_bank_account"

    _settings_major_attrs = ["id", "name"]

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String, unique=True)
    pin: orm.Mapped[str] = orm.mapped_column(sa.String)


class PostTagAssociation(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_post_tag_association"

    post_id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    tag_id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    description: orm.Mapped[str] = orm.mapped_column(sa.String)


class BaseCrudTest:
    engine: sa.Engine = None

    @property
    def eng(self) -> sa.Engine:
        """
        shortcut for ``self.engine``
        """
        return self.engine

    @classmethod
    def setup_class(cls):
        """
        It is called one once before all test method start.

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.class_level_data_setup` please
        """
        if cls.engine is not None:
            metadata.create_all(cls.engine)
            Base.metadata.create_all(cls.engine)
        cls.class_level_data_setup()

    @classmethod
    def teardown_class(cls):
        """
        It is called one once when all test method finished.

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.class_level_data_teardown` please
        """
        cls.class_level_data_teardown()

    def setup_method(self, method):
        """
        It is called before all test method invocation

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.method_level_data_setup` please
        """
        self.method_level_data_setup()

    def teardown_method(self, method):
        """
        It is called after all test method invocation.

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.method_level_data_teardown` please
        """
        self.method_level_data_teardown()

    @classmethod
    def class_level_data_setup(cls):
        """
        Put data preparation task here.
        """
        pass

    @classmethod
    def class_level_data_teardown(cls):
        """
        Put data cleaning task here.
        """
        pass

    def method_level_data_setup(self):
        """
        Put data preparation task here.
        """
        pass

    def method_level_data_teardown(self):
        """
        Put data cleaning task here.
        """
        pass

    @classmethod
    def delete_all_data_in_core_table(cls):
        with cls.engine.connect() as connection:
            connection.execute(t_user.delete())
            connection.execute(t_inv.delete())
            connection.execute(t_cache.delete())
            connection.execute(t_graph.delete())
            connection.execute(t_smart_insert.delete())
            connection.commit()

    @classmethod
    def delete_all_data_in_orm_table(cls):
        with cls.engine.connect() as connection:
            connection.execute(User.__table__.delete())
            connection.execute(Association.__table__.delete())
            connection.execute(Order.__table__.delete())
            connection.execute(BankAccount.__table__.delete())
            connection.execute(PostTagAssociation.__table__.delete())
            connection.commit()
