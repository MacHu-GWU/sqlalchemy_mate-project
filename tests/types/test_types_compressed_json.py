# -*- coding: utf-8 -*-

import sys
import json

import sqlalchemy as sa
import sqlalchemy.orm as orm

from sqlalchemy_mate.types.compressed_json import CompressedJSONType
from sqlalchemy_mate.tests.api import IS_WINDOWS, engine_sqlite, engine_psql

import pytest

Base = orm.declarative_base()


# an example mapping using the base
class Order(Base):
    __tablename__ = "types_compressed_json_orders"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    items: orm.Mapped[str] = orm.mapped_column(CompressedJSONType, nullable=True)


class CompressedJSONBaseTest:
    engine: sa.Engine = None

    id_ = 1
    items = [
        dict(item_id="item_001", item_name="apple", item_count=12),
        dict(item_id="item_002", item_name="banana", item_count=6),
        dict(item_id="item_003", item_name="cherry", item_count=36),
    ]

    @classmethod
    def setup_class(cls):
        if cls.engine is None:
            return

        Base.metadata.drop_all(cls.engine)
        Base.metadata.create_all(cls.engine)

        with cls.engine.connect() as conn:
            conn.execute(Order.__table__.delete())
            conn.commit()

        with orm.Session(cls.engine) as ses:
            order = Order(id=cls.id_, items=cls.items)
            ses.add(order)
            ses.commit()

    def test_read_and_write(self):
        with orm.Session(self.engine) as ses:
            order = ses.get(Order, self.id_)
            assert order.items == self.items

            order = Order(id=2)
            ses.add(order)
            ses.commit()

            order = ses.get(Order, 2)
            assert order.items == None

    def test_underlying_data_is_compressed(self):
        metadata = sa.MetaData()
        t_user = sa.Table(
            "types_compressed_json_orders",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("items", sa.LargeBinary),
        )
        with self.engine.connect() as conn:
            stmt = sa.select(t_user).where(t_user.c.id == self.id_)
            order = conn.execute(stmt).one()
            assert isinstance(order[1], bytes)
            assert sys.getsizeof(order[1]) <= sys.getsizeof(json.dumps(self.items))

    def test_select_where(self):
        with orm.Session(self.engine) as ses:
            order = ses.scalars(sa.select(Order).where(Order.items == self.items)).one()
            assert order.items == self.items


class TestSqlite(CompressedJSONBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPsql(CompressedJSONBaseTest):  # pragma: no cover
    engine = engine_psql

    def test_select_where(self):
        with orm.Session(self.engine) as ses:
            order = ses.scalars(sa.select(Order).where(Order.items == self.items)).one()
            assert order.items == self.items


if __name__ == "__main__":
    from sqlalchemy_mate.tests.helper import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.types.compressed_json", preview=False)
