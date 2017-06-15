#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

from collections import OrderedDict
from sqlalchemy import select, func, distinct

try:
    from ..pkg.prettytable import from_db_cursor
except:
    from sqlalchemy_mate.pkg.prettytable import from_db_cursor


def count_row(engine, table):
    """Return number of rows in a table.

    **中文文档**

    返回一个表中的行数。
    """
    return engine.execute(table.count()).fetchone()[0]


def select_all(engine, table):
    """Select everything from a table.

    **中文文档**

    选取所有数据。
    """
    s = select([table])
    return engine.execute(s)


def select_column(engine, *columns):
    """Select single or multiple columns.

    :param columns: list of sqlalchemy.Column instance

    :returns headers: headers
    :returns data: list of row

    **中文文档**

    - 在选择单列时, 返回的是 str, list
    - 在选择多列时, 返回的是 str list, list of list

    返回单列或多列的数据。
    """
    s = select(columns)
    if len(columns) == 1:
        c_name = columns[0].name
        array = [row[0] for row in engine.execute(s)]
        header, data = c_name, array
        return header, data
    else:
        # from same table
        if len({column.table.name for column in columns}) == 1:
            headers = [column.name for column in columns]
        else:
            headers = [column.__str__() for column in columns]

        data = [tuple(row) for row in engine.execute(s)]
        return headers, data


def select_distinct_column(engine, *columns):
    """Select distinct column(columns).

    :returns: if single column, return list, if multiple column, return matrix.

    **中文文档**

    distinct语句的语法糖函数。
    """
    s = select(columns).distinct()
    if len(columns) == 1:
        c_name = columns[0].name
        return [row[c_name] for row in engine.execute(s)]
    else:
        return [[row[column.name] for column in columns] for row in engine.execute(s)]
