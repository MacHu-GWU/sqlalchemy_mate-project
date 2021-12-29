# -*- coding: utf-8 -*-

import sys
import json
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy_mate.types.compressed import CompressedStringType, CompressedBinaryType
from sqlalchemy_mate.tests import IS_WINDOWS, engine_sqlite, engine_psql

import pytest

Base = declarative_base()


class Url(Base):
    __tablename__ = "types_compressed_urls"

    url = sa.Column(sa.String, primary_key=True)
    html = sa.Column(CompressedStringType)


class Image(Base):
    __tablename__ = "types_compressed_images"

    path = sa.Column(sa.String, primary_key=True)
    content = sa.Column(CompressedBinaryType)


class CompressedBaseTest:
    engine: Engine = None

    html = "<html><head><title>Welcome to Python.org</title></head></html>" * 10
    content = ("cc1297141fcae6c70fce9a9320752a87" * 10).encode("utf-8")

    @classmethod
    def setup_class(cls):
        if cls.engine is None:
            return

        Base.metadata.drop_all(cls.engine)
        Base.metadata.create_all(cls.engine)

        with cls.engine.connect() as conn:
            conn.execute(Url.__table__.delete())
            conn.execute(Image.__table__.delete())

        with Session(cls.engine) as ses:
            ses.add(Url(url="www.python.org", html=cls.html))
            ses.add(Image(path="/tmp/logo.jpg", content=cls.content))
            ses.commit()

    def test_read_and_write(self):
        with Session(self.engine) as ses:
            url = ses.get(Url, "www.python.org")
            assert url.html == self.html

            image = ses.get(Image, "/tmp/logo.jpg")
            assert image.content == self.content

    def test_underlying_data_is_compressed(self):
        metadata = sa.MetaData()
        t_url = sa.Table(
            "types_compressed_urls", metadata,
            sa.Column("url", sa.String, primary_key=True),
            sa.Column("html", sa.LargeBinary),
        )

        t_image = sa.Table(
            "types_compressed_images", metadata,
            sa.Column("path", sa.String, primary_key=True),
            sa.Column("content", sa.LargeBinary),
        )

        with self.engine.connect() as conn:
            stmt = sa.select(t_url).where(t_url.c.url == "www.python.org")
            url = conn.execute(stmt).one()
            assert isinstance(url[1], bytes)
            assert sys.getsizeof(url[1]) <= sys.getsizeof(self.html)

            stmt = sa.select(t_image).where(t_image.c.path == "/tmp/logo.jpg")
            image = conn.execute(stmt).one()
            assert isinstance(image[1], bytes)
            assert sys.getsizeof(image[1]) <= sys.getsizeof(self.content)

    def test_select_where(self):
        with Session(self.engine) as ses:
            url = ses.scalars(
                sa.select(Url).where(Url.html == self.html)
            ).one()
            assert url.html == self.html

            image = ses.scalars(
                sa.select(Image).where(Image.content == self.content)
            ).one()
            assert image.content == self.content


class TestSqlite(CompressedBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPsql(CompressedBaseTest):  # pragma: no cover
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
