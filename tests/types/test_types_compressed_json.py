# -*- coding: utf-8 -*-

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy_mate.types.compressed_json import CompressedJSONType
from sqlalchemy_mate.tests import IS_WINDOWS, engine_sqlite, engine_psql

Base = declarative_base()


# an example mapping using the base
class Order(Base):
    __tablename__ = "types_compressed_json_orders"

    id = sa.Column(sa.Integer, primary_key=True)
    items = sa.Column(CompressedJSONType)


class CompressedJSONBaseTest:
    engine: Engine = None

    id_ = 1
    items = [
        dict(item_id="item_001", item_name="apple", item_count=12),
        dict(item_id="item_002", item_name="banana", item_count=6),
        dict(item_id="item_003", item_name="cherry", item_count=36),
    ]

    @classmethod
    def setup_class(cls):
        if cls.engine is not None:
            Base.metadata.create_all(cls.engine)
            order = Order(id=cls.id_, items=cls.items)
            with Session(cls.engine) as ses:
                if ses.get(Order, 1) is None:
                    ses.add(order)
                    ses.commit()

    def test_read_and_write(self):
        with Session(self.engine) as ses:
            order = ses.get(Order, self.id_)
            assert order.items == self.items


class TestSqlite(CompressedJSONBaseTest):
    engine = engine_sqlite

    def test_select_where(self):
        with Session(self.engine) as ses:
            order = ses.scalars(sa.select(Order).where(Order.items == self.items)).one()
            assert order.items == self.items


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPsql(CompressedJSONBaseTest):  # pragma: no cover
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
