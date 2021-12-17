# -*- coding: utf-8 -*-

import pytest


def test():
    import sqlalchemy_mate as sm

    _ = sm.selecting.count_row
    _ = sm.inserting.smart_insert
    _ = sm.updating.upsert_all

    _ = sm.io.sql_to_csv
    _ = sm.pt.from_everything

    _ = sm.ExtendedBase
    _ = sm.EngineCreator
    _ = sm.test_connection
    _ = sm.TimeoutError


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
