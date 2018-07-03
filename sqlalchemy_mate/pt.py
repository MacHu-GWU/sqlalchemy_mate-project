#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pretty Table support.
"""

from sqlalchemy import select, Table
from sqlalchemy_mate.pkg.prettytable import from_db_cursor
from sqlalchemy.orm import sessionmaker, Query
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.sql.selectable import Select
from .utils import execute_query_return_result_proxy


def from_sql(sql, engine, limit=None):
    """Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.select`.

    **中文文档**

    将sqlalchemy的query结果放入prettytable中。
    """
    # 注意, from_db_cursor是从原生的数据库游标通过调用fetchall()方法来获取数据。
    # 而sqlalchemy返回的是ResultProxy类。所以我们需要从中获取游标
    # 至于为什么不能直接使用 from_db_cursor(engine.execute(sql).cursor) 的语法
    # 我也不知道为什么
    if limit is not None:
        sql = sql.limit(limit)
    result_proxy = engine.execute(sql)
    return from_db_cursor(result_proxy.cursor)


def from_query(query, engine=None, limit=None):
    if limit is not None:
        query = query.limit(limit)
    result_proxy = execute_query_return_result_proxy(query)
    return from_db_cursor(result_proxy.cursor)


def from_table(table, engine, limit=None):
    """Select data in a database table and put into prettytable.

    Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.Table`.

    **中文文档**

    将数据表中的数据放入prettytable中.
    """
    sql = select([table])
    if limit is not None:
        sql = sql.limit(limit)
    result_proxy = engine.execute(sql)
    return from_db_cursor(result_proxy.cursor)


def from_object(orm_class, engine, limit=None):
    """Select data from the table defined by a ORM class, and put into prettytable

    :param orm_class: an orm class inherit from ``sqlalchemy.ext.declarative.declarative_base()``

    **中文文档**

    将数据对象的数据放入prettytable中.
    """
    Session = sessionmaker(bind=engine)
    ses = Session()
    query = ses.query(orm_class)
    if limit is not None:
        query = query.limit(limit)
    result_proxy = execute_query_return_result_proxy(query)
    ses.close()
    return from_db_cursor(result_proxy.cursor)


def from_everything(everything, engine, limit=None):
    if isinstance(everything, Table):
        return from_table(everything, engine, limit=limit)

    try:
        if issubclass(everything, DeclarativeMeta):
            return from_object(everything, engine, limit=limit)
    except TypeError:
        pass

    if isinstance(everything, Query):
        return from_query(everything, engine, limit=limit)

    if isinstance(everything, Select):
        return from_sql(everything, engine, limit=limit)
