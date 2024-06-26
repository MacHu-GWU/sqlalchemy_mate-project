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

import typing as T

import sqlalchemy as sa
import sqlalchemy.orm as orm

from prettytable import PrettyTable

from .utils import ensure_session, clean_session


def get_keys_values(
    item,
) -> T.Tuple[T.Union[list, tuple], T.Union[list, tuple]]:
    if isinstance(item.__class__, orm.DeclarativeMeta):
        keys, values = list(), list()
        for column in item.__class__.__table__.columns:
            keys.append(column.name)
            values.append(getattr(item, column.name))
        return keys, values
    elif isinstance(item, sa.Row):
        return list(item._fields), list(item)
    else:  # pragma: no cover
        raise TypeError


def from_result(result: T.Union[sa.Result, sa.ScalarResult]):
    pt = PrettyTable()
    try:
        first_item = next(result)
        keys, values = get_keys_values(first_item)
        pt.field_names = keys
        pt.add_row(values)
    except StopIteration:
        return pt

    if isinstance(result, sa.ScalarResult):
        for obj in result:
            pt.add_row([getattr(obj, key) for key in keys])
    elif isinstance(result, sa.Result):
        for row in result:
            pt.add_row(list(row))
    else:
        raise Exception

    return pt


def from_text_clause(t: sa.TextClause, engine: sa.Engine, **kwargs) -> PrettyTable:
    """
    Execute a query in form of texture clause, return the result in form of
    :class:`PrettyTable`.
    """
    with engine.connect() as connection:
        result = connection.execute(t, **kwargs)
        return from_result(result)


def from_stmt(stmt: sa.Select, engine: sa.Engine, **kwargs) -> PrettyTable:
    """
    Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.select`.

    :param sql: a ``sqlalchemy.sql.selectable.Select`` object.
    :param engine: an ``sqlalchemy.engine.base.Engine`` object.

    **中文文档**

    将 sqlalchemy 的 sql expression query 结果放入 prettytable 中.
    """
    with engine.connect() as connection:
        result = connection.execute(stmt, **kwargs)
        return from_result(result)


def from_table(
    table: sa.Table, engine: sa.Engine, limit: int = None, **kwargs
) -> PrettyTable:
    """
    Select data in a database table and put into prettytable.

    Create a :class:`prettytable.PrettyTable` from :class:`sqlalchemy.Table`.

    :param table: a ``sqlalchemy.sql.schema.Table`` object
    :param engine: the engine or session used to execute the query.
    :param limit: int, limit rows to return.

    **中文文档**

    将数据表中的数据放入 PrettyTable 中.
    """
    stmt = sa.select(table)
    if limit is not None:
        stmt = stmt.limit(limit)
    with engine.connect() as connection:
        result = connection.execute(stmt, **kwargs)
        return from_result(result)


def from_model(
    orm_class,
    engine_or_session: T.Union[sa.Engine, orm.Session],
    limit: int = None,
    **kwargs
):
    """
    Select data from the table defined by a ORM class, and put into prettytable

    :param orm_class: an orm class inherit from
        ``sqlalchemy.ext.declarative.declarative_base()``
    :param engine_or_session: the engine or session used to execute the query.
    :param limit: int, limit rows to return.

    **中文文档**

    将数据对象的数据放入 PrettyTable 中.

    常用于快速预览某个对象背后的数据.
    """
    ses, auto_close = ensure_session(engine_or_session)
    stmt = sa.select(orm_class.__table__)
    if limit is not None:
        stmt = stmt.limit(limit)
    result = ses.execute(stmt, **kwargs)
    tb = from_result(result)
    clean_session(ses, auto_close)
    return tb


def from_dict_list(data: T.List[dict]) -> PrettyTable:
    """
    Construct a Prettytable from list of dictionary.
    """
    tb = PrettyTable()
    if len(data) == 0:  # pragma: no cover
        return tb
    else:
        tb.field_names = list(data[0].keys())
        for row in data:
            tb.add_row(list(row.values()))
        return tb


def from_everything(
    everything: T.Union[
        sa.TextClause,
        sa.Select,
        sa.Table,
        T.List[dict],
        orm.DeclarativeMeta,
    ],
    engine_or_session: T.Union[sa.Engine, orm.Session] = None,
    limit: int = None,
    **kwargs
):
    """
    Construct a Prettytable from any kinds of sqlalchemy query.

    :type engine_or_session: Union[Engine, Session]

    :rtype: PrettyTable

    Usage::

        from sqlalchemy import select

        sql = select(t_user)
        print(from_everything(sql, engine))

        query = session.query(User)
        print(from_everything(query, session))

        session.query(User)
    """
    if isinstance(everything, sa.TextClause):
        return from_text_clause(everything, engine_or_session, **kwargs)

    if isinstance(everything, str):
        return from_text_clause(sa.text(everything), engine_or_session, **kwargs)

    if isinstance(everything, sa.Select):
        return from_stmt(everything, engine_or_session, **kwargs)

    if isinstance(everything, sa.Table):
        return from_table(everything, engine_or_session, limit=limit, **kwargs)

    if isinstance(everything, orm.DeclarativeMeta):
        return from_model(everything, engine_or_session, limit=limit, **kwargs)

    if isinstance(everything, sa.Result):
        return from_result(everything)

    if isinstance(everything, list):
        return from_dict_list(everything)

    raise Exception
