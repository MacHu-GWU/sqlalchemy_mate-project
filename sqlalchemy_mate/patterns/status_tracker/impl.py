# -*- coding: utf-8 -*-


import typing as T
import enum
import uuid
import traceback
import dataclasses
from contextlib import contextmanager
from datetime import datetime, timezone

import sqlalchemy as sa
import sqlalchemy.orm as orm


EPOCH = datetime(1970, 1, 1)


class JobLockedError(Exception):
    pass


class JobIgnoredError(Exception):
    pass


class JobMixin:
    """
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
    def create_and_save(
        cls,
        engine: sa.Engine,
        id: str,
        status: int,
        data: T.Optional[dict] = None,
        **kwargs,
    ):
        job = cls.create(id=id, status=status, data=data, **kwargs)
        with orm.Session(engine) as ses:
            ses.add(job)
            ses.commit()
        return job

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
        if debug:
            print(
                f"🔓Try to set status = {in_progress_status} and lock the job {self.id!r} ..."
            )
        klass = self.__class__
        lock = uuid.uuid4().hex
        utc_now = datetime.utcnow()
        stmt = (
            sa.update(klass)
            .where(klass.id == self.id, klass.lock == None)
            .values(lock=lock, lock_at=utc_now, status=in_progress_status)
        )
        res = ses.execute(stmt)
        ses.commit()
        if res.rowcount == 1:
            if debug:
                print("  Successfully lock the job!")
        else:
            if debug:
                print("  Failed to lock the job")
            raise JobLockedError
        return lock, utc_now

    def lock_it(
        self,
        engine_or_session: T.Union[sa.Engine, orm.Session],
        in_progress_status: int,
        debug: bool = False,
    ) -> T.Tuple[str, datetime]:
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
        if debug:
            print("{msg:-^80}".format(msg=(f" ▶️ start Job {id!r}")))

        updates = Updates()

        with orm.Session(engine) as ses:
            job = ses.get(cls, id)
            if job is None: # pragma: no cover
                raise ValueError

            if job.is_locked(expire=expire):
                raise JobLockedError(f"Job {id!r} is locked.")

            if job.status == ignore_status:
                if debug:
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
                if debug:
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
                    if debug:
                        print(
                            f"❌ 🔐 job failed {max_retry} times already, "
                            f"set status = {ignore_status} and unlock the job."
                        )
                    failed_updates.values["status"] = ignore_status
                else:
                    if debug:
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
                if debug:
                    status = updates.values["status"]
                    print(
                        "{msg:-^80}".format(
                            msg=(f" ⏹️ end Job {id!r} status = {status})")
                        )
                    )
                # print("before finally")


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
    values: dict = dataclasses.field(default_factory=dict)

    def set(self, key: str, value: T.Any):
        if key in disallowed_cols: # pragma: no cover
            raise KeyError(f"You should NOT set {key!r} column yourself!")
        self.values[key] = value
