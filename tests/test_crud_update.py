#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import String, Integer, Float
from sqlalchemy_mate.crud import update
from sqlalchemy_mate.crud import select


def test_upsert_all():
    # Prepare table
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    t_upsert_all = Table("t_upsert_all", metadata,
                         Column("id", String(), primary_key=True),
                         Column("value", Integer()),
                         )
    metadata.create_all(engine)

    # Insert some initial data
    ins = t_upsert_all.insert()
    data = [
        {"id": "a1"},
    ]
    engine.execute(ins, data)

    # Upsert all
    data = [
        {"id": "a1", "value": 1},  # This will update
        {"id": "a2", "value": 2},  # This will insert
    ]
    update.upsert_all(engine, t_upsert_all, data)

    assert list(select.select_all(engine, t_upsert_all)) == [
        ('a1', 1), ('a2', 2)]


if __name__ == "__main__":
    import os
    pytest.main([os.path.basename(__file__), "--tb=native", "-s", ])
