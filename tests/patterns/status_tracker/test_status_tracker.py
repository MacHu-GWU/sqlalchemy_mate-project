# -*- coding: utf-8 -*-

import enum

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm

from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
)
from sqlalchemy_mate.patterns.status_tracker.impl import (
    JobLockedError,
    JobIgnoredError,
    JobMixin,
    Updates,
)


class StatusEnum(int, enum.Enum):
    pending = 10
    in_progress = 20
    failed = 30
    succeeded = 40
    ignored = 50


Base = orm.declarative_base()

from sqlalchemy_mate.orm.extended_declarative_base import ExtendedBase


class Job(Base, ExtendedBase, JobMixin):
    __tablename__ = "jobs"

    @classmethod
    def start_job(
        cls,
        engine: sa.Engine,
        id: str,
        skip_error: bool = False,
        debug: bool = False,
    ):
        return cls.start(
            engine=engine,
            id=id,
            in_process_status=StatusEnum.in_progress.value,
            failed_status=StatusEnum.failed.value,
            success_status=StatusEnum.succeeded.value,
            ignore_status=StatusEnum.ignored.value,
            expire=900,
            max_retry=3,
            skip_error=skip_error,
            debug=debug,
        )


job_id = "job-1"


class MyError(Exception):
    pass


class StatusTrackerBaseTest:
    engine: sa.Engine = None

    @classmethod
    def setup_class(cls):
        Base.metadata.create_all(cls.engine)

    def setup_method(self):
        with self.engine.connect() as conn:
            conn.execute(Job.__table__.delete())
            conn.commit()

    def get_job(self):
        with orm.Session(self.engine) as ses:
            return ses.get(Job, job_id)

    def test_create(self):
        job = Job.create(id=job_id, status=StatusEnum.pending.value)
        job = self.get_job()
        assert job is None

    def test_create_and_save(self):
        with orm.Session(self.engine) as ses:
            job = Job.create_and_save(
                engine_or_session=ses,
                id=job_id,
                status=StatusEnum.pending.value,
            )
            assert job.data == None

        job = self.get_job()
        assert isinstance(job, Job)

    def test_lock_it(self):
        # create a new job
        job = Job.create_and_save(
            engine_or_session=self.engine,
            id=job_id,
            status=StatusEnum.pending.value,
        )

        # then get the job from db, it should NOT be locked
        job = self.get_job()
        assert job.is_locked(expire=10) is False

        # then lock it, the in-memory job object should be locked
        job.lock_it(
            engine_or_session=self.engine,
            in_progress_status=StatusEnum.in_progress.value,
        )
        assert job.status == StatusEnum.in_progress.value
        assert job.lock is not None
        assert job.is_locked(expire=10) is True

        # then get the job from db, it should be locked
        job = self.get_job()
        assert job.status == StatusEnum.in_progress.value
        assert job.lock is not None
        assert job.is_locked(expire=10) is True

    def test_update(self):
        Job.create_and_save(
            engine_or_session=self.engine,
            id=job_id,
            status=StatusEnum.pending.value,
        )

        job = Job.create(id=job_id, status=StatusEnum.pending.value)
        updates = Updates()
        updates.set(key="data", value={"version": 1})
        job.update(engine_or_session=self.engine, updates=updates)

        job = self.get_job()
        assert job.data == {"version": 1}

    def _create_and_save_for_start(self):
        """
        Create an initial job so that we can test the ``start(...)`` method.
        """
        Job.create_and_save(
            engine_or_session=self.engine,
            id=job_id,
            status=StatusEnum.pending.value,
            data={"version": 1},
        )

    def test_start_and_succeeded(self):
        self._create_and_save_for_start()
        with Job.start_job(engine=self.engine, id=job_id, debug=False) as (
            job,
            updates,
        ):
            updates.set(key="data", value={"version": job.data["version"] + 1})

        job = self.get_job()
        assert job.status == StatusEnum.succeeded.value
        assert job.lock == None
        assert job.data == {"version": 2}
        # print(job.__dict__)

    def test_start_and_failed(self):
        self._create_and_save_for_start()
        with pytest.raises(MyError):
            with Job.start_job(engine=self.engine, id=job_id, debug=False) as (
                job,
                updates,
            ):
                updates.set(key="data", value={"version": job.data["version"] + 1})
                raise MyError
        job = self.get_job()
        assert job.status == StatusEnum.failed.value
        assert job.lock == None
        assert job.data == {"version": 1}
        # print(job.__dict__)

    def test_start_and_ignored(self):
        self._create_and_save_for_start()
        with Job.start_job(
            engine=self.engine, id=job_id, skip_error=True, debug=False
        ) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
            raise Exception
        with Job.start_job(
            engine=self.engine, id=job_id, skip_error=True, debug=False
        ) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
            raise Exception
        with Job.start_job(
            engine=self.engine, id=job_id, skip_error=True, debug=False
        ) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
            raise Exception
        job = self.get_job()
        assert job.status == StatusEnum.ignored.value
        assert job.lock == None
        assert job.data == {"version": 1}
        # print(job.__dict__)

        with pytest.raises(JobIgnoredError):
            with Job.start_job(
                engine=self.engine, id=job_id, skip_error=True, debug=False
            ) as (
                job,
                updates,
            ):
                updates.set(key="data", value={"version": job.data["version"] + 1})

    def test_start_and_concurrent_worker_conflict(self):
        self._create_and_save_for_start()
        with Job.start_job(engine=self.engine, id=job_id, debug=False) as (
            job1,
            updates1,
        ):
            with pytest.raises(JobLockedError):
                with Job.start_job(engine=self.engine, id=job_id, debug=False) as (
                    job2,
                    updates2,
                ):
                    updates2.set(
                        key="data",
                        value={"version": job2.data["version"] + 100},
                    )
            updates1.set(key="data", value={"version": job1.data["version"] + 1})

        job = self.get_job()
        assert job.status == StatusEnum.succeeded.value
        assert job.lock == None
        assert job.data == {"version": 2}
        # print(job.__dict__)

    def test_query_by_status(self):
        self._create_and_save_for_start()

        job_list = Job.query_by_status(
            engine_or_session=self.engine,
            status=StatusEnum.pending.value,
        )
        assert len(job_list) == 1
        assert job_list[0].id == job_id

        with orm.Session(self.engine) as ses:
            job_list = Job.query_by_status(
                engine_or_session=ses,
                status=StatusEnum.pending.value,
            )
        assert len(job_list) == 1
        assert job_list[0].id == job_id


class TestSqlite(StatusTrackerBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPsql(StatusTrackerBaseTest):  # pragma: no cover
    engine = engine_psql


class TestUpdates:
    def test(self):
        updates = Updates()
        with pytest.raises(KeyError):
            updates.set(key="status", value=0)


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_mate.patterns.status_tracker.impl",
        preview=False,
    )
