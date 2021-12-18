# -*- coding: utf-8 -*-

import sqlalchemy_mate as sam

engine_sqlite = sam.EngineCreator().create_sqlite(path="/tmp/db.sqlite")
sam.test_connection(engine_sqlite, timeout=1)

engine_psql = sam.EngineCreator(
    username="postgres",
    password="password",
    database="postgres",
    host="localhost",
    port=43347,
).create_postgresql_psycopg2()
sam.test_connection(engine_sqlite, timeout=3)
