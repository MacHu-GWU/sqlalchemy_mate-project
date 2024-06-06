# -*- coding: utf-8 -*-

import typing as T
import uuid
import traceback
import dataclasses
from contextlib import contextmanager
from datetime import datetime, timedelta

import sqlalchemy as sa
import sqlalchemy.orm as orm


EPOCH = datetime(1970, 1, 1)


class JobExecutionError(Exception):
    pass


class JobLockedError(JobExecutionError):
    """
    Raised when try to start a locked job.
    """

    pass


class JobIsNotReadyToStartError(JobExecutionError):
    """
    Raised when try to start job that the current status shows that it is not
    ready to start.
    """

    pass


class JobAlreadySucceededError(JobIsNotReadyToStartError):
    """
    Raised when try to start a succeeded (failed too many times) job.
    """

    pass


class JobIgnoredError(JobIsNotReadyToStartError):
    """
    Raised when try to start an ignored (failed too many times) job.
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

    注 1, 这里我们故意没有用 ``SELECT ... WHERE ... FOR UPDATE`` 的行锁语法, 因为我们
    需要显式的维护这个锁的开关和生命周期.

    注 2, 我们是先获得这个 job, 检查是否上锁, 然后再 update 上锁. 你可能会担心在检查成功后
    到 update 上锁期间如果有其他人把这个锁锁上了怎么办? 这个问题是不存在的, 因为 update 里的
    where 会保证如果尝试上锁的时候已经被上锁了, 这个 update 会失败. 再一个你可能会问为什么不
    先 update 上锁, 再获取 job. 因为我们希望当这个 job 已经被上锁时, 其他的并发 worker 能够
    用最小的代价了解到这个 job 已经被上锁了. 而明显 get 的代价比 update 要小得多, 所以
    优先用 get 来获得 job 检查锁的状态.
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
        # if someone else locked the job before us, we will enter this branch
        else:  # pragma: no cover
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
        pending_status: int,
        in_progress_status: int,
        failed_status: int,
        succeeded_status: int,
        ignored_status: int,
        expire: int,
        max_retry: int,
        more_pending_status: T.Optional[T.Union[int, T.List[int]]] = None,
        traceback_stack_limit: int = 10,
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
                pending_status=10,
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
        :param id: unique job id, usually the primary key of the job table.
            todo, add support to allow compound primary key.
        :param pending_status: pending status code in integer.
        :param in_progress_status: in_progress status code in integer.
        :param failed_status: failed status code in integer.
        :param succeeded_status: succeeded status code in integer.
        :param ignored_status: ignored status code in integer.
        :param more_pending_status: additional pending status code that logically
            equal to "pending" status.
        :param max_retry: how many retry is allowed before we ignore it
        :param expire: how long the lock will expire
        :param skip_error: if True, ignore the error during the job execution logics.
            note that this flag won't ignore the error during the context manager
            start up and clean up. For example, it won't ignore the :class:`JobLockedError`.
        :param debug: if True, print debug message.

        注: 这里的设计跟 pynamodb_mate 中的 status tracker 模块不同. 这里没有
        detailed_error 这个参数. 这是因为在 sql 中我们会先 get job 再 update 获取锁, 所以
        在获取锁失败时我们无需再次查询数据库来了解错误原因. 而 dynamodb 是先 update 获取锁,
        出错后如需了解详细的错误原因需要一次额外的 get 操作.
        """
        if debug:  # pragma: no cover
            print("{msg:-^80}".format(msg=(f" ▶️ start Job {id!r}")))

        updates = Updates()

        with orm.Session(engine) as ses:
            job: T.Optional["T_JOB"] = ses.get(cls, id)
            if job is None:  # pragma: no cover
                raise ValueError

            if job.is_locked(expire=expire):
                if debug:  # pragma: no cover
                    print(f"❌ Job {id!r} is locked.")
                raise JobLockedError(f"Job {id!r} is locked.")

            ready_to_start_status = [
                pending_status,
                failed_status,
            ]
            if more_pending_status is None:
                pass
            elif isinstance(more_pending_status, int):
                ready_to_start_status.append(more_pending_status)
            else:
                ready_to_start_status.extend(more_pending_status)

            if job.status not in ready_to_start_status:
                if job.status == succeeded_status:
                    if debug:  # pragma: no cover
                        print(f"❌ Job {id!r} is already succeeded, do nothing.")
                    raise JobAlreadySucceededError(
                        f"Job {id!r} is already succeeded, do nothing."
                    )
                elif job.status == ignored_status:
                    if debug:  # pragma: no cover
                        print(f"❌ Job {id!r} is ignored, do nothing.")
                    raise JobIgnoredError(
                        f"Job {id!r} retry count already exceeded {max_retry}, "
                        f"ignore it."
                    )
                elif job.status not in ready_to_start_status:
                    if debug:  # pragma: no cover
                        print(
                            f"❌ Job {id!r} status is {job.status}, "
                            f"it is not any of the ready-to-start status: {ready_to_start_status}."
                        )
                    raise JobIsNotReadyToStartError(
                        f"Job {id!r} status is {job.status}, "
                        f"it is not any of the ready-to-start status: {ready_to_start_status}."
                    )
                else:
                    raise NotImplementedError(
                        f"You found a bug! This error should be handled but not implemented yet, "
                        f"please report to https://github.com/MacHu-GWU/sqlalchemy_mate-project/issues;"
                    )

            _, _ = job.lock_it(
                engine_or_session=ses,
                in_progress_status=in_progress_status,
                debug=debug,
            )
            updates.values["status"] = in_progress_status

            try:
                # print("before yield")
                yield job, updates
                # print("after yield")
                if debug:  # pragma: no cover
                    print(
                        f"✅ 🔐 job succeeded, "
                        f"set status = {succeeded_status} and unlock the job."
                    )
                updates.values["status"] = succeeded_status
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
                            f"set status = {ignored_status} and unlock the job."
                        )
                    failed_updates.values["status"] = ignored_status
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
                    "traceback": traceback.format_exc(limit=traceback_stack_limit),
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
