# -*- coding: utf-8 -*-


def test():
    import sqlalchemy_mate.api as sm

    _ = sm.selecting.count_row
    _ = sm.selecting.by_pk
    _ = sm.selecting.select_all
    _ = sm.selecting.select_single_column
    _ = sm.selecting.select_many_column
    _ = sm.selecting.select_single_distinct
    _ = sm.selecting.select_many_distinct
    _ = sm.selecting.select_random
    _ = sm.selecting.yield_tuple
    _ = sm.selecting.yield_dict
    _ = sm.inserting.smart_insert
    _ = sm.updating.update_all
    _ = sm.updating.upsert_all
    _ = sm.deleting.delete_all

    _ = sm.test_connection
    _ = sm.EngineCreator
    _ = sm.ExtendedBase
    _ = sm.TimeoutError

    _ = sm.io.sql_to_csv
    _ = sm.io.table_to_csv
    _ = sm.pt.from_result
    _ = sm.pt.from_text_clause
    _ = sm.pt.from_stmt
    _ = sm.pt.from_table
    _ = sm.pt.from_model
    _ = sm.pt.from_dict_list
    _ = sm.pt.from_everything

    _ = sm.patterns.status_tracker.JobLockedError
    _ = sm.patterns.status_tracker.JobIgnoredError
    _ = sm.patterns.status_tracker.JobMixin
    _ = sm.patterns.status_tracker.Updates


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.api", preview=False)
