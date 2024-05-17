# -*- coding: utf-8 -*-

import typing as T
import uuid
import traceback
import dataclasses
from contextlib import contextmanager
from datetime import datetime

import sqlalchemy as sa
import sqlalchemy.orm as orm


EPOCH = datetime(1970, 1, 1)


class JobLockedError(Exception):
    """
    Raised when try to start a locked job.
    """

    pass


class JobIgnoredError(Exception):
    """
    Raised when try to start a ignored (failed too many times) job.
    """

    pass


class JobMixin:
    """
    The sqlalchemy ORM data model mixin class that brings in status tracking
    related features. Core API includes:

    - :meth:`JobMixin.create`
    - :meth:`JobMixin.create_and_save`
    - :meth:`JobMixin.start`
    - :meth:`JobMixin.query_by_status`

    See: https://docs.sqlalchemy.org/en/20/orm/declarative_mixins.html

    **锁机制**

    为了在高并发的环境中防止多个程序执行同一个 Job, 我们需要对 Job 进行加锁.

    每当我们准备执行一个 Job 时, 我们需要根据 job id 去获取一把锁 (也就是将 job 上锁).
    然后执行 Job 的业务逻辑. 在 Job 正在执行的过程中, 其他程序是不允许彭这个 Job 的.
    整个流程的时间顺序如下:

    1. 用 ``SELECT ... WHERE id = 'job_id'`` 获取这个 Job.
    2. 看看这个 Job 是否上锁, 如果已上锁则直接退出.
    3. 用 ``UPDATE ... SET lock = ... WHERE id = 'job_id' AND lock IS NULL`` 来
        进行加锁操作. 如果在 #1 之后, #3 之前有人把这个 Job 锁上了, 这个 SQL 就不会执行成功,
        我们也就视为获取锁失败.

    注, 这里我们故意没有用 ``SELECT ... WHERE ... FOR UPDATE`` 的行锁语法, 因为我们
    需要显式的维护这个锁的开关和生命周期.
    """

    # fmt: off
    id: orm.Mapped[str] = orm.mapped_column(sa.String, primary_key=True)
    status: orm.Mapped[int] = orm.mapped_column(sa.Integer)
    create_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    update_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    lock: orm.Mapped[T.Optional[str]] = orm.mapped_column(sa.String, nullable=True)
    lock_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    retry: orm.Mapped[int] = orm.mapped_column(sa.Integer)
    data: orm.Mapped[dict] = orm.mapped_column(sa.JSON, nullable=True)
    errors: orm.Mapped[dict] = orm.mapped_column(sa.JSON, nullable=True)
    # fmt: on

    @classmethod
    def create(
        cls,
        id: str,
        status: int,
        data: T.Optional[dict] = None,
        **kwargs,
    ):
        """
        Create an in-memory instance of the job object. This method won't write
        the job to database. This is useful for initializing many new jobs in batch.

        Usage example::

            with orm.Session(engine) as ses:
                for job_id in job_id_list:
                    job = Job.create(id=job_id, status=10)
                    ses.add(job)
                ses.commit()
        """
        utc_now = datetime.utcnow()
        return cls(
            id=id,
            status=status,
            create_at=utc_now,
            update_at=utc_now,
            lock=None,
            lock_at=EPOCH,
            retry=0,
            data=data,
            errors={},
            **kwargs,
        )

    @classmethod
    def _create_and_save(
        cls,
        ses: orm.Session,
        id: str,
        status: int,
        data: T.Optional[dict] = None,
        **kwargs,
    ):
        job = cls.create(id=id, status=status, data=data, **kwargs)
        ses.add(job)
        ses.commit()
        return job

    @classmethod
    def create_and_save(
        cls,
        engine_or_session: T.Union[sa.Engine, orm.Session],
        id: str,
        status: int,
        data: T.Optional[dict] = None,
        **kwargs,
    ):
        """
        Create an instance of the job object and write the job to database.

        Usage example::

            with orm.Session(engine) as ses:
                Job.create_and_save(ses, id="job-1", status=10)
        """
        if isinstance(engine_or_session, sa.Engine):
            with orm.Session(engine_or_session) as ses:
                return cls._create_and_save(
                    ses=ses,
                    id=id,
                    status=status,
                    data=data,
                    **kwargs,
                )
        else:
            return cls._create_and_save(
                ses=engine_or_session,
                id=id,
                status=status,
                data=data,
                **kwargs,
            )

    def is_locked(self, expire: int) -> bool:
        """
        Check if the job is locked.
        """
        if self.lock is None:
            return False
        else:
            utc_now = datetime.utcnow()
            return (utc_now - self.lock_at).total_seconds() < expire

    def _lock_it(
        self,
        ses: orm.Session,
        in_progress_status: int,
        debug: bool = False,
    ) -> T.Tuple[str, datetime]:
        if debug:  # pragma: no cover
            print(
                f"🔓Try to set status = {in_progress_status} and lock the job {self.id!r} ..."
            )
        klass = self.__class__
        lock = uuid.uuid4().hex
        utc_now = datetime.utcnow()
        stmt = (
            sa.update(klass)
            .where(klass.id == self.id, klass.lock == None)
            .values(status=in_progress_status, lock=lock, lock_at=utc_now)
        )
        res = ses.execute(stmt)
        ses.commit()
        if res.rowcount == 1:
            self.status = in_progress_status
            self.lock = lock
            self.lock_at = utc_now
            if debug:  # pragma: no cover
                print("  Successfully lock the job!")
        else:
            if debug:  # pragma: no cover
                print("  Failed to lock the job")
            raise JobLockedError(f"Job {self.id!r}")
        return lock, utc_now

    def lock_it(
        self,
        engine_or_session: T.Union[sa.Engine, orm.Session],
        in_progress_status: int,
        debug: bool = False,
    ) -> T.Tuple[str, datetime]:
        """
        :return: a tuple of ``(lock, lock_at)``.
        """
        if isinstance(engine_or_session, sa.Engine):
            with orm.Session(engine_or_session) as ses:
                return self._lock_it(
                    ses=ses,
                    in_progress_status=in_progress_status,
                    debug=debug,
                )
        else:
            return self._lock_it(
                ses=engine_or_session,
                in_progress_status=in_progress_status,
                debug=debug,
            )

    def _update(
        self,
        ses: orm.Session,
        updates: "Updates",
    ):
        klass = self.__class__
        stmt = sa.update(klass).where(klass.id == self.id).values(**updates.values)
        # we may need to add try, except, rollback here
        ses.execute(stmt)
        ses.commit()

    def update(
        self,
        engine_or_session: T.Union[sa.Engine, orm.Session],
        updates: "Updates",
    ):
        if isinstance(engine_or_session, sa.Engine):
            with orm.Session(engine_or_session) as ses:
                self._update(ses=ses, updates=updates)
        else:
            self._update(ses=engine_or_session, updates=updates)

    @classmethod
    @contextmanager
    def start(
        cls,
        engine: sa.Engine,
        id: str,
        in_process_status: int,
        failed_status: int,
        success_status: int,
        ignore_status: int,
        expire: int,
        max_retry: int,
        skip_error: bool = False,
        debug: bool = False,
    ) -> T.ContextManager[T.Tuple["T_JOB", "Updates"]]:
        """
        This is the most important API. A context manager that does a lot of things:

        1. Try to obtain lock before the job begin. Once we have obtained the lock,
            other work won't be able to update this row (they will see that it is locked).
        2. Any raised exception will be captured by the context manager, and it will
            set the status as failed, add retry count, log the error
            (and save the error information to DB), and release the lock.
        3. If the job has been failed too many times, it will set the status as ``ignored``.
        4. If everything goes well, it will set status as ``succeeded`` and apply updates.

        Usage example::

            with Job.start(
                engine=engine,
                id="job-1",
                in_process_status=20,
                failed_status=30,
                success_status=40,
                ignore_status=50,
                expire=900, # concurrency lock will expire in 15 minutes,
                max_retry=3,
                debug=True,
            ) as (job, updates):
                # do your job logic here
                ...
                # you can use ``updates.set(...)`` method to specify
                # what you would like to update at the end of the job
                # if the job succeeded.
                updates.set(key="data", value={"version": 1})

        :param engine: SQLAlchemy engine. A life-cycle of a job has to be done
            in a new session.
        """
        if debug:  # pragma: no cover
            print("{msg:-^80}".format(msg=(f" ▶️ start Job {id!r}")))

        updates = Updates()

        with orm.Session(engine) as ses:
            job = ses.get(cls, id)
            if job is None:  # pragma: no cover
                raise ValueError

            if job.is_locked(expire=expire):
                if debug:  # pragma: no cover
                    print(f"Job {id!r} is locked.")
                raise JobLockedError(f"Job {id!r} is locked.")

            if job.status == ignore_status:
                if debug:  # pragma: no cover
                    print(f"↪️ the job is ignored, do nothing!")
                raise JobIgnoredError(
                    f"Job {id!r} retry count already exceeded {max_retry}, "
                    f"ignore it."
                )

            lock, lock_at = job.lock_it(
                engine_or_session=ses,
                in_progress_status=in_process_status,
                debug=debug,
            )
            updates.values["status"] = in_process_status

            try:
                # print("before yield")
                yield job, updates
                # print("after yield")
                if debug:  # pragma: no cover
                    print(
                        f"✅ 🔐 job succeeded, "
                        f"set status = {success_status} and unlock the job."
                    )
                updates.values["status"] = success_status
                updates.values["update_at"] = datetime.utcnow()
                updates.values["lock"] = None
                updates.values["retry"] = 0
                job.update(engine_or_session=ses, updates=updates)
            except Exception as e:
                # print("before error handling")
                failed_updates = Updates()
                if job.retry + 1 >= max_retry:
                    if debug:  # pragma: no cover
                        print(
                            f"❌ 🔐 job failed {max_retry} times already, "
                            f"set status = {ignore_status} and unlock the job."
                        )
                    failed_updates.values["status"] = ignore_status
                else:
                    if debug:  # pragma: no cover
                        print(
                            f"❌ 🔐 job failed, "
                            f"set status = {failed_status} and unlock the job."
                        )
                    failed_updates.values["status"] = failed_status
                failed_updates.values["update_at"] = datetime.utcnow()
                failed_updates.values["lock"] = None
                failed_updates.values["errors"] = {
                    "error": repr(e),
                    "traceback": traceback.format_exc(limit=10),
                }
                failed_updates.values["retry"] = job.retry + 1
                job.update(engine_or_session=ses, updates=failed_updates)
                # print("after error handling")
                if skip_error is False:
                    raise e
            finally:
                if debug:  # pragma: no cover
                    status = updates.values["status"]
                    print(
                        "{msg:-^80}".format(
                            msg=(f" ⏹️ end Job {id!r} status = {status})")
                        )
                    )
                # print("before finally")

    @classmethod
    def _query_by_status(
        cls,
        ses: orm.Session,
        status: int,
        limit: int = 10,
        older_task_first: bool = True,
    ) -> T.List["T_JOB"]:
        where_clauses = list()
        where_clauses.append(cls.status == status)
        stmt = sa.select(cls).where(cls.status == status)
        if older_task_first:
            stmt = stmt.order_by(sa.asc(cls.update_at))
        stmt = stmt.limit(limit)
        return ses.scalars(stmt).all()

    @classmethod
    def query_by_status(
        cls,
        engine_or_session: T.Union[sa.Engine, orm.Session],
        status: int,
        limit: int = 10,
        older_task_first: bool = True,
    ) -> T.List["T_JOB"]:
        """
        Query job by status.

        :param engine_or_session:
        :param status: desired status code
        :param limit: number of jobs to return
        :param older_task_first: if True, then return older task
        (older update_at time) first.
        """
        if isinstance(engine_or_session, sa.Engine):
            with orm.Session(engine_or_session) as ses:
                job_list = cls._query_by_status(
                    ses, status=status, limit=limit, older_task_first=older_task_first
                )
        else:
            job_list = cls._query_by_status(
                engine_or_session,
                status=status,
                limit=limit,
                older_task_first=older_task_first,
            )
        return job_list


T_JOB = T.TypeVar("T_JOB", bound=JobMixin)


disallowed_cols = {
    "id",
    "status",
    "create_at",
    "update_at",
    "lock",
    "lock_at",
    "retry",
}


@dataclasses.dataclass
class Updates:
    """
    A helper class that hold the key value you want to update at the end of the
    job if the job succeeded.
    """

    values: dict = dataclasses.field(default_factory=dict)

    def set(self, key: str, value: T.Any):
        """
        Use this method to set "to-update" data. Note that you should not
        update some columns like "id", "status", "update_at" yourself,
        it will be updated by the :meth:`JobMixin.start` context manager.
        """
        if key in disallowed_cols:  # pragma: no cover
            raise KeyError(f"You should NOT set {key!r} column yourself!")
        self.values[key] = value
