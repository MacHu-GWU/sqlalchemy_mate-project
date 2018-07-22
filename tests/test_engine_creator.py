#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from sqlalchemy_mate import engine_creator


def test_create_sqlite():
    assert engine_creator._create_sqlite() == "sqlite:///:memory:"
    assert engine_creator._create_sqlite("test.db") == \
        "sqlite:///test.db"


username = "admin"
password = "password"
host = "localhost"
port = 8080
database = "test"


def test_postgresql():
    assert engine_creator._create_postgresql(
        username, password, host, port, database) == \
        "postgresql://admin:password@localhost:8080/test"

    assert engine_creator._create_postgresql(
        username, password, host, None, database) == \
        "postgresql://admin:password@localhost/test"

    assert engine_creator._create_postgresql_psycopg2(
        username, password, host, port, database) == \
        "postgresql+psycopg2://admin:password@localhost:8080/test"

    assert engine_creator._create_postgresql_pg8000(
        username, password, host, port, database) == \
        "postgresql+pg8000://admin:password@localhost:8080/test"

    assert engine_creator._create_postgresql_pygresql(
        username, password, host, port, database) == \
        "postgresql+pygresql://admin:password@localhost:8080/test"

    assert engine_creator._create_postgresql_psycopg2cffi(
        username, password, host, port, database) == \
        "postgresql+psycopg2cffi://admin:password@localhost:8080/test"

    assert engine_creator._create_postgresql_pypostgresql(
        username, password, host, port, database) == \
        "postgresql+pypostgresql://admin:password@localhost:8080/test"


def test_mysql():
    assert engine_creator._create_mysql(
        username, password, host, port, database) == \
        "mysql://admin:password@localhost:8080/test"

    assert engine_creator._create_mysql_mysqldb(
        username, password, host, port, database) == \
        "mysql+mysqldb://admin:password@localhost:8080/test"

    assert engine_creator._create_mysql_mysqlconnector(
        username, password, host, port, database) == \
        "mysql+mysqlconnector://admin:password@localhost:8080/test"

    assert engine_creator._create_mysql_oursql(
        username, password, host, port, database) == \
        "mysql+oursql://admin:password@localhost:8080/test"

    assert engine_creator._create_mysql_pymysql(
        username, password, host, port, database) == \
        "mysql+pymysql://admin:password@localhost:8080/test"

    assert engine_creator._create_mysql_cymysql(
        username, password, host, port, database) == \
        "mysql+cymysql://admin:password@localhost:8080/test"


def test_oracle():
    assert engine_creator._create_oracle(
        username, password, host, port, database) == \
        "oracle://admin:password@localhost:8080/test"

    assert engine_creator._create_oracle_cx_oracle(
        username, password, host, port, database) == \
        "oracle+cx_oracle://admin:password@localhost:8080/test"


def test_mssql():
    assert engine_creator._create_mssql_pyodbc(
        username, password, host, port, database) == \
        "mssql+pyodbc://admin:password@localhost:8080/test"

    assert engine_creator._create_mssql_pymssql(
        username, password, host, port, database) == \
        "mssql+pymssql://admin:password@localhost:8080/test"


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
