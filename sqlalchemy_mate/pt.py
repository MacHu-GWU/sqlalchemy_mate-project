# -*- coding: utf-8 -*-

"""
Pretty Table support. Allow you quickly print query result in ascii table.

:func:`from_everything`

**中文文档**

因为 PrettyTable 只能通过 from_db_cursor 来创建, 也就是说要求返回
ResultProxy, 而不是 ORM 对象的列表. 所以在 :func:`from_object`, :func:`from_query`
这些使用 ``session.query(Entity)`` 的函数中, 我们需要用特殊技巧, 让 session.query
最终返回 ResultProxy.
"""

try:
    from typing import Union, Any
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm.session import Session
except ImportError:  # pragma: no cover
    pass

from sqlalchemy import select, Table
from sqlalchemy.orm import Query
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.engine.result import ResultProxy

from .utils import ensure_session, execute_query_return_result_proxy
from .pkg.prettytable import from_db_cursor, PrettyTable


def from_stmt(stmt, engine, **kwargs):
    """
    Execute a query in form of texture clause, return the result in form of

    :class:`PrettyTable`.

    :type stmt: TextClause
    :param stmt:

    :type engine: Engine
    :param engine:

    :rtype: PrettyTable
    """
    result_proxy = engine.execute(stmt, **kwargs)
    return from_db_cursor(result_proxy.cursor)


def from_sql(sql, engine, limit=None, **kwargs):
    """
    Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.select`.

    :type sql: Select
    :param sql: a ``sqlalchemy.sql.selectable.Select`` object.

    :type engine: Engine
    :param engine: an ``sqlalchemy.engine.base.Engine`` object.

    :type limit: int
    :param limit: int, limit rows to return.

    :rtype: PrettyTable

    **中文文档**

    将sqlalchemy的sql expression query结果放入prettytable中.

    .. note::

        注意, from_db_cursor是从原生的数据库游标通过调用fetchall()方法来获取数据。
        而sqlalchemy返回的是ResultProxy类。所以我们需要从中获取游标
        至于为什么不能直接使用 from_db_cursor(engine.execute(sql).cursor) 的语法
        我也不知道为什么.
    """
    if limit is not None:
        sql = sql.limit(limit)
    result_proxy = engine.execute(sql)
    return from_db_cursor(result_proxy.cursor)


def from_query(query, engine_or_session=None, limit=None, **kwargs):
    """
    Execute an ORM style query, and return the result in
    :class:`prettytable.PrettyTable`.

    :type query: Query
    :param query: an ``sqlalchemy.orm.Query`` object.

    :type engine: Any
    :param engine: query is always associated with a session, this parameter
        just reserve the arg place.

    :type limit: int
    :param limit: int, limit rows to return.

    :rtype: PrettyTable
    :return: a ``prettytable.PrettyTable`` object

    **中文文档**

    将通过 ORM 的查询结果中的数据放入 :class:`PrettyTable` 中.
    """
    if limit is not None:
        query = query.limit(limit)
    result_proxy = execute_query_return_result_proxy(query)
    return from_db_cursor(result_proxy.cursor)


def from_table(table, engine, limit=None, **kwargs):
    """
    Select data in a database table and put into prettytable.

    Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.Table`.

    :type table: Table
    :param table: a ``sqlalchemy.sql.schema.Table`` object

    :type engine: Engine
    :param engine: the engine or session used to execute the query.

    :type limit: int
    :param limit: int, limit rows to return.

    :rtype: PrettyTable

    **中文文档**

    将数据表中的数据放入 PrettyTable 中.
    """
    sql = select([table])
    if limit is not None:
        sql = sql.limit(limit)
    result_proxy = engine.execute(sql, **kwargs)
    return from_db_cursor(result_proxy.cursor)


def from_object(orm_class, engine_or_session, limit=None, **kwargs):
    """
    Select data from the table defined by a ORM class, and put into prettytable

    :param orm_class: an orm class inherit from
        ``sqlalchemy.ext.declarative.declarative_base()``

    :type engine_or_session: Union[Engine, Session]
    :param engine_or_session: the engine or session used to execute the query.

    :type limit: int
    :param limit: int, limit rows to return.

    :rtype: PrettyTable

    **中文文档**

    将数据对象的数据放入 PrettyTable 中.

    常用于快速预览某个对象背后的数据.
    """

    ses, auto_close = ensure_session(engine_or_session)
    query = ses.query(orm_class)
    if limit is not None:
        query = query.limit(limit)
    result_proxy = execute_query_return_result_proxy(query)
    if auto_close:
        ses.close()
    return from_db_cursor(result_proxy.cursor)


def from_resultproxy(result_proxy, **kwargs):
    """
    Construct a Prettytable from ``ResultProxy``.

    :param result_proxy: a ``sqlalchemy.engine.result.ResultProxy`` object.
    """
    return from_db_cursor(result_proxy.cursor)


def from_data(data):
    """
    Construct a Prettytable from list of rows.

    :rtype: PrettyTable
    """
    if len(data) == 0:  # pragma: no cover
        return None
    else:
        ptable = PrettyTable()
        ptable.field_names = data[0].keys()
        for row in data:
            ptable.add_row(row)
        return ptable


def from_everything(everything, engine_or_session, limit=None, **kwargs):
    """
    Construct a Prettytable from any kinds of sqlalchemy query.

    :type engine_or_session: Union[Engine, Session]

    :rtype: PrettyTable

    Usage::

        from sqlalchemy import select

        sql = select([t_user])
        print(from_everything(sql, engine))

        query = session.query(User)
        print(from_everything(query, session))

        session.query(User)
    """
    if isinstance(everything, TextClause):
        return from_stmt(everything, engine_or_session, **kwargs)

    if isinstance(everything, Table):
        return from_table(everything, engine_or_session, limit=limit, **kwargs)

    if type(everything) is DeclarativeMeta:
        return from_object(everything, engine_or_session, limit=limit, **kwargs)

    if isinstance(everything, Query):
        return from_query(everything, engine_or_session, limit=limit, **kwargs)

    if isinstance(everything, Select):
        return from_sql(everything, engine_or_session, limit=limit, **kwargs)

    if isinstance(everything, ResultProxy):
        return from_resultproxy(everything, **kwargs)

    if isinstance(everything, list):
        return from_data(everything, **kwargs)
