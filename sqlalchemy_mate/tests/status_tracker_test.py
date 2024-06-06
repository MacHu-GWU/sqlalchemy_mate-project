# -*- coding: utf-8 -*-

import enum

import sqlalchemy.orm as orm

from sqlalchemy_mate.orm.api import ExtendedBase
from sqlalchemy_mate.patterns.status_tracker.api import (
    JobMixin,
)
from sqlalchemy_mate.tests.api import engine_psql as engine


class StatusEnum(int, enum.Enum):
    pending = 10
    in_progress = 20
    failed = 30
    succeeded = 40
    ignored = 50


Base = orm.declarative_base()


class Job(Base, ExtendedBase, JobMixin):
    __tablename__ = "sqlalchemy_mate_status_tracker_job"

    @classmethod
    def start_job(
        cls,
        id: str,
        skip_error: bool = False,
        debug: bool = False,
    ):
        return cls.start(
            engine=engine,
            id=id,
            in_progress_status=StatusEnum.in_progress.value,
            failed_status=StatusEnum.failed.value,
            succeeded_status=StatusEnum.succeeded.value,
            ignored_status=StatusEnum.ignored.value,
            expire=15,
            max_retry=3,
            skip_error=skip_error,
            debug=debug,
        )


Base.metadata.create_all(engine)
