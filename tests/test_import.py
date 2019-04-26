# -*- coding: utf-8 -*-

import pytest


def test():
    import sqlalchemy_mate as sm

    sm.selecting.count_row
    sm.inserting.smart_insert
    sm.updating.upsert_all

    sm.engine_creator.create_sqlite

    sm.io.sql_to_csv
    sm.pt.from_everything

    sm.ExtendedBase
    sm.Credential
    sm.EngineCreator
    sm.test_connection
    sm.TimeoutError


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
