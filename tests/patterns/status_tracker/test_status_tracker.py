# -*- coding: utf-8 -*-

import enum

import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm

from sqlalchemy_mate.tests.api import engine_psql as engine
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


class Job(Base, JobMixin):
    __tablename__ = "jobs"

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
            in_process_status=StatusEnum.in_progress.value,
            failed_status=StatusEnum.failed.value,
            success_status=StatusEnum.succeeded.value,
            ignore_status=StatusEnum.ignored.value,
            expire=900,
            max_retry=3,
            skip_error=skip_error,
            debug=debug,
        )


Base.metadata.create_all(engine)

job_id = "job-1"


class MyError(Exception):
    pass


class TestJob:
    def setup_method(self):
        with engine.connect() as conn:
            conn.execute(Job.__table__.delete())
            conn.commit()

    def get_job(self):
        with orm.Session(engine) as ses:
            return ses.get(Job, job_id)

    def test_create(self):
        job = Job.create(id=job_id, status=StatusEnum.pending.value)
        job = self.get_job()
        assert job is None

    def test_create_and_save(self):
        job = Job.create_and_save(
            id=job_id, status=StatusEnum.pending.value, engine=engine
        )
        job = self.get_job()
        assert isinstance(job, Job)

    def test_lock_it(self):
        job = Job.create_and_save(
            id=job_id,
            status=StatusEnum.pending.value,
            engine=engine,
        )
        job = self.get_job()
        assert job.is_locked(expire=10) is False
        job.lock_it(
            engine_or_session=engine,
            in_progress_status=StatusEnum.in_progress.value,
            debug=True,
        )
        job = self.get_job()

        assert job.is_locked(expire=10) is True
        assert job.status == StatusEnum.in_progress.value

    def test_update(self):
        Job.create_and_save(
            id=job_id,
            status=StatusEnum.pending.value,
            engine=engine,
        )

        job = Job.create(id=job_id, status=StatusEnum.pending.value)
        updates = Updates()
        updates.set(key="data", value={"version": 1})
        job.update(engine_or_session=engine, updates=updates)

        job = self.get_job()
        assert job.data == {"version": 1}

    def _create_and_save_for_start(self):
        Job.create_and_save(
            id=job_id,
            status=StatusEnum.pending.value,
            data={"version": 1},
            engine=engine,
        )

    def test_start_and_succeeded(self):
        self._create_and_save_for_start()
        with Job.start_job(id=job_id, debug=True) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
        job = self.get_job()
        assert job.status == StatusEnum.succeeded.value
        assert job.lock == None
        assert job.data == {"version": 2}
        # print(job.__dict__)

    def test_start_and_failed(self):
        self._create_and_save_for_start()
        with pytest.raises(MyError):
            with Job.start_job(id=job_id, debug=True) as (job, updates):
                updates.set(key="data", value={"version": job.data["version"] + 1})
                raise MyError
        job = self.get_job()
        assert job.status == StatusEnum.failed.value
        assert job.lock == None
        assert job.data == {"version": 1}
        # print(job.__dict__)

    def test_start_and_ignored(self):
        self._create_and_save_for_start()
        with Job.start_job(id=job_id, skip_error=True, debug=True) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
            raise Exception
        with Job.start_job(id=job_id, skip_error=True, debug=True) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
            raise Exception
        with Job.start_job(id=job_id, skip_error=True, debug=True) as (job, updates):
            updates.set(key="data", value={"version": job.data["version"] + 1})
            raise Exception
        job = self.get_job()
        assert job.status == StatusEnum.ignored.value
        assert job.lock == None
        assert job.data == {"version": 1}
        # print(job.__dict__)

        with pytest.raises(JobIgnoredError):
            with Job.start_job(id=job_id, skip_error=True, debug=True) as (
                job,
                updates,
            ):
                updates.set(key="data", value={"version": job.data["version"] + 1})

    def test_start_and_concurrent_worker_conflict(self):
        self._create_and_save_for_start()
        with Job.start_job(id=job_id, debug=True) as (job1, updates1):
            print("first worker start the job")
            with pytest.raises(JobLockedError):
                with Job.start_job(id=job_id, skip_error=True, debug=True) as (
                    job2,
                    updates2,
                ):
                    print("second worker start the job")
                    updates2.set(
                        key="data", value={"version": job2.data["version"] + 100}
                    )
            updates1.set(key="data", value={"version": job1.data["version"] + 1})

        job = self.get_job()
        assert job.status == StatusEnum.succeeded.value
        assert job.lock == None
        assert job.data == {"version": 2}
        # print(job.__dict__)


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_mate.patterns.status_tracker.impl",
        preview=False,
    )
