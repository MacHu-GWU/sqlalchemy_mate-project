#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module provide utility functions for select operation.
"""

from sqlalchemy import select, func

try:
    from ..pkg.prettytable import from_db_cursor
except:  # pragma: no cover
    from sqlalchemy_mate.pkg.prettytable import from_db_cursor


def count_row(engine, table):
    """
    Return number of rows in a table.

    **中文文档**

    返回一个表中的行数。
    """
    return engine.execute(table.count()).fetchone()[0]


def select_all(engine, table):
    """
    Select everything from a table.

    **中文文档**

    选取所有数据。
    """
    s = select([table])
    return engine.execute(s)


def select_single_column(engine, column):
    s = select([column])
    return column.name, [row[0] for row in engine.execute(s)]


def select_many_column(engine, *columns):
    """
    Select single or multiple columns.

    :param columns: list of sqlalchemy.Column instance

    :returns headers: headers
    :returns data: list of row

    **中文文档**

    - 在选择单列时, 返回的是 str, list
    - 在选择多列时, 返回的是 str list, list of list

    返回单列或多列的数据。
    """
    s = select(columns)
    headers = [str(column) for column in columns]
    data = [tuple(row) for row in engine.execute(s)]
    return headers, data


def select_distinct_column(engine, *columns):
    """
    Select distinct column(columns).

    :returns: if single column, return list, if multiple column, return matrix.

    **中文文档**

    distinct语句的语法糖函数。
    """
    s = select(columns).distinct()
    if len(columns) == 1:
        return [row[0] for row in engine.execute(s)]
    else:
        return [tuple(row) for row in engine.execute(s)]


def select_random(engine, table_or_columns, limit=5):
    """
    Randomly select some rows from table.
    """
    s = select(table_or_columns).order_by(func.random()).limit(limit)
    return engine.execute(s).fetchall()
