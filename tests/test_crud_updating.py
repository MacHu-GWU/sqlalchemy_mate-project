# -*- coding: utf-8 -*-

import pytest
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import String, Integer
from sqlalchemy_mate.crud import updating
from sqlalchemy_mate.crud import selecting


def test_upsert_all_single_primary_key_column():
    # Prepare table
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    t_upsert_all = Table(
        "t_upsert_all_value", metadata,
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
    updating.upsert_all(engine, t_upsert_all, data)

    assert list(selecting.select_all(engine, t_upsert_all)) == [
        ('a1', 1), ('a2', 2)]


def test_upsert_all_multiple_primary_key_column():
    # Prepare table
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    t_upsert_all = Table(
        "t_upsert_all_movie", metadata,
        Column("movei_id", Integer, primary_key=True),
        Column("tag_id", Integer, primary_key=True),
        Column("value", Integer),
    )
    metadata.create_all(engine)

    # Insert some initial data
    ins = t_upsert_all.insert()
    data = [
        {"movei_id": 1, "tag_id": 1, "value": 0},
    ]
    engine.execute(ins, data)

    # Upsert all
    data = [
        {"movei_id": 1, "tag_id": 1, "value": 1},  # This will update
        {"movei_id": 3, "tag_id": 1, "value": 5},  # This will insert
    ]
    updating.upsert_all(engine, t_upsert_all, data)

    assert list(selecting.select_all(engine, t_upsert_all)) == [
        (1, 1, 1), (3, 1, 5)]


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
