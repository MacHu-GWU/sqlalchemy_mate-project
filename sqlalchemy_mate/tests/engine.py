# -*- coding: utf-8 -*-

import sqlalchemy as sa

from ..engine_creator import EngineCreator


# use make run-psql to run postgres container on local
engine_sqlite = sa.create_engine("sqlite:///:memory:")

engine_psql = EngineCreator(
    username="postgres",
    password="password",
    database="postgres",
    host="localhost",
    port=40311,
).create_postgresql_pg8000()
