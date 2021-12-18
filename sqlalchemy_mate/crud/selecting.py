# -*- coding: utf-8 -*-

"""
This module provide utility functions for select operation.
"""

from typing import List, Union, Iterable
from sqlalchemy import select, func, Column, Table, and_
from sqlalchemy.engine import Engine, Result, Row
from ..utils import ensure_exact_one_arg_is_not_none


def count_row(
    engine: Engine,
    table: Table,
) -> int:
    """
    Return number of rows in a table.

    Example:

        >>> import sqlalchemy as sa
        >>> t_user = sa.Table()
        >>> count_row(engine, t_user)
        3

    **中文文档**

    返回一个表中的行数。
    """
    with engine.connect() as connection:
        return connection.execute(
            select([func.count()]).select_from(table)
        ).fetchone()[0]


def by_pk(
    engine: Engine,
    table: Table,
    id_,
) -> Union[Row, None]:
    """
    Return single row or None by primary key values.
    """
    with engine.connect() as connection:
        if isinstance(id_, (tuple, list)):
            if len(id_) != len(table.primary_key):
                raise ValueError
            where_args = list()
            for column, value in zip(table.primary_key, id_):
                where_args.append(column == value)
            return connection.execute(
                select(table).where(and_(*where_args))
            ).fetchone()
        else:
            if len(table.primary_key) != 1:
                raise ValueError
            return connection.execute(
                select(table).where(list(table.primary_key)[0] == id_)
            ).fetchone()


def select_all(
    engine: Engine,
    table: Table,
) -> Result:
    """
    Select everything from a table.
    """
    s = select([table])
    with engine.connect() as connection:
        return connection.execute(s)


def select_single_column(
    engine: Engine,
    column: Column,
) -> list:
    """
    Select data from single column.
    """
    s = select([column])
    with engine.connect() as connection:
        return [row[0] for row in connection.execute(s)]


def select_many_column(
    engine: Engine,
    columns: List[Column],
) -> List[tuple]:
    """
    Select data from multiple columns.
    """
    s = select(columns)
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(s)]


def select_single_distinct(
    engine: Engine,
    column: Column,
) -> list:
    """
    Select distinct data from single column.
    """
    s = select([column]).distinct()
    with engine.connect() as connection:
        return [row[0] for row in connection.execute(s)]


def select_many_distinct(
    engine: Engine,
    columns: List[Column],
) -> List[tuple]:
    """
    Select distinct data from multiple columns.
    """
    s = select(columns).distinct()
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(s)]


def select_random(
    engine: Engine,
    table: Table = None,
    columns: List[Column] = None,
    limit: int = None,
    perc: int = None
) -> Result:
    """
    Randomly select some rows from table.
    """
    ensure_exact_one_arg_is_not_none(limit, perc)
    ensure_exact_one_arg_is_not_none(table, columns)

    if table is not None:
        if limit is not None:
            stmt = select(table).order_by(func.random()).limit(limit)
        else:
            if perc >= 100 or perc <= 0:
                raise ValueError

            selectable = table.tablesample(
                func.bernoulli(perc),
                name="alias",
                seed=func.random()
            )
            args = [
                getattr(selectable.c, column.name)
                for column in table.columns
            ]
            stmt = select(*args)
    elif columns is not None:
        if limit is not None:
            stmt = select(columns).order_by(func.random()).limit(limit)
        else:
            if perc >= 100 or perc <= 0:
                raise ValueError
            selectable = columns[0].table.tablesample(
                func.bernoulli(perc),
                name="alias",
                seed=func.random()
            )
            args = [
                getattr(selectable.c, column.name)
                for column in columns
            ]
            stmt = select(*args)
    else:  # pragma: no cover, for readability only
        raise NotImplementedError

    with engine.connect() as connection:
        return connection.execute(stmt)


def yield_tuple(result: Result) -> Iterable[tuple]:
    for row in result:
        yield tuple(row)


def yield_dict(result: Result) -> Iterable[dict]:
    for row in result:
        yield dict(row)
