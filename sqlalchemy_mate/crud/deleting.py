# -*- coding: utf-8 -*-

"""
This module provide utility functions for delete operation.
"""

import sqlalchemy as sa


def delete_all(
    engine: sa.Engine,
    table: sa.Table,
):
    """
    Delete all data in a table.
    """
    with engine.connect() as connection:
        connection.execute(table.delete())
        connection.commit()
