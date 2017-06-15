#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pretty Table support.
"""

from sqlalchemy import select
from sqlalchemy_mate.pkg.prettytable import from_db_cursor


def from_sql(sql, engine):
    """Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.select`.

    **中文文档**

    将sqlalchemy的query结果放入prettytable中。
    """
    # 注意, from_db_cursor是从原生的数据库游标通过调用fetchall()方法来获取数据。
    # 而sqlalchemy返回的是ResultProxy类。所以我们需要从中获取游标
    # 至于为什么不能直接使用 from_db_cursor(engine.execute(sql).cursor) 的语法
    # 我也不知道为什么
    result_proxy = engine.execute(sql)
    return from_db_cursor(result_proxy.cursor)


def from_table(table, engine):
    """Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.Table`.

    **中文文档**

    将数据表中的数据放入prettytable中。
    """
    sql = select([table])
    result_proxy = engine.execute(sql)
    return from_db_cursor(result_proxy.cursor)
