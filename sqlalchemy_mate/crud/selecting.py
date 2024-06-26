# -*- coding: utf-8 -*-

"""
This module provide utility functions for select operation.
"""

import typing as T

import sqlalchemy as sa

from ..utils import ensure_exact_one_arg_is_not_none


def count_row(
    engine: sa.Engine,
    table: sa.Table,
) -> int:
    """
    Return number of rows in a table.

    Example::

        import sqlalchemy as sa
        import sqlalchemy_mate as sam

        t_users = sa.Table(...)
        engine = sa.create_engine(...)
        sam.selecting.count_row(engine, t_user)

    **中文文档**

    返回一个表中的行数。
    """
    with engine.connect() as connection:
        return connection.execute(
            sa.select(sa.func.count()).select_from(table)
        ).fetchone()[0]


def by_pk(
    engine: sa.Engine,
    table: sa.Table,
    id_,
) -> T.Union[sa.Row, None]:
    """
    Return single row or None by primary key values.

    :param id_: single value if table has only one primary key, tuple / list of
        values if table has multiple primary keys, positioning is sensitive.

    Example::

        import sqlalchemy as sa
        import sqlalchemy_mate as sam

        t_users = sa.Table
            "users", metadata,
            Column("id", sa.Integer, primary_key=True),
            ...
        )
        engine = sa.create_engine(...)
        row = sam.selecting.by_pk(engine, t_user, 1) # one row or None
        print(row._fields)      # keys
        print(tuple(row))       # values
        print(row._asdict())    # dict view
    """
    with engine.connect() as connection:
        if isinstance(id_, (tuple, list)):
            if len(id_) != len(table.primary_key):
                raise ValueError
            where_args = list()
            for column, value in zip(table.primary_key, id_):
                where_args.append(column == value)
            stmt = sa.select(table).where(sa.and_(*where_args))
            return connection.execute(stmt).fetchone()
        else:
            if len(table.primary_key) != 1:
                raise ValueError
            stmt = sa.select(table).where(list(table.primary_key)[0] == id_)
            return connection.execute(stmt).fetchone()


def select_all(
    engine: sa.Engine,
    table: sa.Table,
) -> sa.Result:
    """
    Select all rows from a table.

    Example::

        for row in sam.selecting.select_all(engine, t_users):
            ...
    """
    s = sa.select(table)
    with engine.connect() as connection:
        return connection.execute(s)


def select_single_column(
    engine: sa.Engine,
    column: sa.Column,
) -> list:
    """
    Select data from single column.

    Example::

        id_list = sam.selecting.select_all(engine, t_users.c.id)
    """
    s = sa.select(column)
    with engine.connect() as connection:
        return [row[0] for row in connection.execute(s)]


def select_many_column(
    engine: sa.Engine,
    columns: T.List[sa.Column],
) -> T.List[tuple]:
    """
    Select data from multiple columns.

    Example::

        dataframe = sam.selecting.select_all(engine, [t_users.c.id, t_users.c.name])
    """
    s = sa.select(*columns)
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(s)]


def select_single_distinct(
    engine: sa.Engine,
    column: sa.Column,
) -> list:
    """
    Select distinct data from single column.

    Example::

        unique_name_list = sam.selecting.select_all(engine, t_users.c.name)
    """
    s = sa.select(column).distinct()
    with engine.connect() as connection:
        return [row[0] for row in connection.execute(s)]


def select_many_distinct(
    engine: sa.Engine,
    columns: T.List[sa.Column],
) -> T.List[tuple]:
    """
    Select distinct data from multiple columns.

    Example::

        dataframe = sam.selecting.select_many_distinct(engine, [t_users.c.id, t_users.c.name])
    """
    s = sa.select(*columns).distinct()
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(s)]


def select_random(
    engine: sa.Engine,
    table: sa.Table = None,
    columns: T.List[sa.Column] = None,
    limit: int = None,
    perc: int = None,
) -> sa.Result:
    """
    Randomly select some rows from table.

    :param perc: int from 1 ~ 99. (means 1% ~ 99%)

    Example::

        # randomly select 100 users
        for row in sam.selecting.select_random(engine, t_users, limit=100):
            ...

        # randomly select 5% rows from users table
        for row in sam.selecting.select_random(engine, t_users, perc=5):
            ...
    """
    ensure_exact_one_arg_is_not_none(limit, perc)
    ensure_exact_one_arg_is_not_none(table, columns)

    if table is not None:
        if limit is not None:
            stmt = sa.select(table).order_by(sa.func.random()).limit(limit)
        else:
            if perc >= 100 or perc <= 0:
                raise ValueError

            selectable = table.tablesample(
                sa.func.bernoulli(perc), name="alias", seed=sa.func.random()
            )
            args = [getattr(selectable.c, column.name) for column in table.columns]
            stmt = sa.select(*args)
    elif columns is not None:
        if limit is not None:
            stmt = sa.select(*columns).order_by(sa.func.random()).limit(limit)
        else:
            if perc >= 100 or perc <= 0:
                raise ValueError
            selectable = columns[0].table.tablesample(
                sa.func.bernoulli(perc), name="alias", seed=sa.func.random()
            )
            args = [getattr(selectable.c, column.name) for column in columns]
            stmt = sa.select(*args)
    else:  # pragma: no cover, for readability only
        raise NotImplementedError

    with engine.connect() as connection:
        return connection.execute(stmt)


def yield_tuple(result: sa.Result) -> T.Iterable[tuple]:
    """
    Yield rows in tuple values view.
    """
    for row in result:
        yield tuple(row)


def yield_dict(result: sa.Result) -> T.Iterable[dict]:
    """
    Yield rows in dict view.
    """
    for row in result:
        yield row._asdict()
