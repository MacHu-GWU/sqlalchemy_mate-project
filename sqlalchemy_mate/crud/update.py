#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

from collections import OrderedDict
from sqlalchemy import and_


def upsert_all(engine, table, data):
    """Update data by primary key columns. If not able to update, do insert.

    **中文文档**

    """
    ins = table.insert()
    upd = table.update()

    # Find all primary key columns
    pk_cols = OrderedDict()
    for column in table._columns:
        if column.primary_key:
            pk_cols[column.name] = column

    data_to_insert = list()

    # Multiple primary key column
    if len(pk_cols) >= 2:
        for row in data:
            result = engine.execute(
                upd.
                where(
                    and_(
                        *[col == row[name] for name, col in pk_cols.items()]
                    )
                ).
                values(**row)
            )
            if result.rowcount == 0:
                data_to_insert.append(row)

    # Single primary key column
    elif len(pk_cols) == 1:
        for row in data:
            result = engine.execute(
                upd.
                where(
                    [col == row[name] for name, col in pk_cols.items()][0]
                ).
                values(**row)
            )
            if result.rowcount == 0:
                data_to_insert.append(row)

    else:
        data_to_insert = data

    # Insert rest of data
    if len(data_to_insert):
        engine.execute(ins, data_to_insert)
