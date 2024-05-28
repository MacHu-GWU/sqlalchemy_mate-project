# -*- coding: utf-8 -*-


def test():
    import sqlalchemy_mate.api as sam

    _ = sam.selecting.count_row
    _ = sam.selecting.by_pk
    _ = sam.selecting.select_all
    _ = sam.selecting.select_single_column
    _ = sam.selecting.select_many_column
    _ = sam.selecting.select_single_distinct
    _ = sam.selecting.select_many_distinct
    _ = sam.selecting.select_random
    _ = sam.selecting.yield_tuple
    _ = sam.selecting.yield_dict
    _ = sam.inserting.smart_insert
    _ = sam.updating.update_all
    _ = sam.updating.upsert_all
    _ = sam.deleting.delete_all

    _ = sam.test_connection
    _ = sam.EngineCreator
    _ = sam.ExtendedBase
    _ = sam.TimeoutError

    _ = sam.io.sql_to_csv
    _ = sam.io.table_to_csv
    _ = sam.pt.from_result
    _ = sam.pt.from_text_clause
    _ = sam.pt.from_stmt
    _ = sam.pt.from_table
    _ = sam.pt.from_model
    _ = sam.pt.from_dict_list
    _ = sam.pt.from_everything

    _ = sam.patterns.status_tracker.JobLockedError
    _ = sam.patterns.status_tracker.JobIgnoredError
    _ = sam.patterns.status_tracker.JobMixin
    _ = sam.patterns.status_tracker.Updates

    _ = sam.patterns.large_binary_column.aws_s3
    from .aws_s3 import PutS3BackedColumnResult
    from .aws_s3 import put_s3backed_column
    from .aws_s3 import clean_up_created_s3_object_when_create_or_update_row_failed
    from .aws_s3 import clean_up_old_s3_object_when_update_row_succeeded
    from .aws_s3 import PutS3ApiCall
    from .aws_s3 import PutS3Result
    from .aws_s3 import put_s3


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(__file__, "sqlalchemy_mate.api", preview=False)
