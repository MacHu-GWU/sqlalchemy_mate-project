# -*- coding: utf-8 -*-

import pytest

from datetime import datetime

import moto
from s3pathlib import S3Path, context

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy_mate.patterns.large_binary_column import aws_s3_api as aws_s3
from sqlalchemy_mate.tests.mock_aws import BaseMockTest
from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
)


def get_utc_now() -> datetime:
    return datetime.utcnow()


Base = orm.declarative_base()


class Task(Base):
    __tablename__ = "tasks"

    url = orm.mapped_column(sa.String, primary_key=True)
    update_at = orm.mapped_column(sa.DateTime)
    html = orm.mapped_column(sa.String, nullable=True)
    image = orm.mapped_column(sa.String, nullable=True)


bucket: str = "mybucket"
s3dir_root = S3Path(
    f"s3://{bucket}/projects/sqlalchemy_mate/patterns/s3backed_column/data/"
).to_dir()


class BaseTest(BaseMockTest):
    engine: sa.Engine = None

    mock_list = [
        moto.mock_s3,
    ]

    @classmethod
    def setup_class_post_hook(cls):
        Base.metadata.create_all(cls.engine)
        context.attach_boto_session(cls.bsm.boto_ses)
        cls.bsm.s3_client.create_bucket(Bucket=bucket)
        s3dir_root.delete()

    def setup_method(self):
        with self.engine.connect() as conn:
            conn.execute(Task.__table__.delete())
            conn.commit()

    def test(self):
        engine = self.engine
        bsm = self.bsm

        url = "https://www.example.com"
        html_content_1 = b"<html>this is html 1</html>"
        image_content_1 = b"this is image 1"
        html_additional_kwargs = dict(ContentType="text/html")
        image_additional_kwargs = dict(ContentType="image/jpeg")
        utc_now = get_utc_now()

        put_s3_result = aws_s3.put_s3(
            api_calls=[
                aws_s3.PutS3ApiCall(
                    column="html",
                    binary=html_content_1,
                    old_s3_uri=None,
                    extra_put_object_kwargs=html_additional_kwargs,
                ),
                aws_s3.PutS3ApiCall(
                    column="image",
                    binary=image_content_1,
                    old_s3_uri=None,
                    extra_put_object_kwargs=image_additional_kwargs,
                ),
            ],
            s3_client=bsm.s3_client,
            pk=url,
            bucket=s3dir_root.bucket,
            prefix=s3dir_root.key,
            update_at=utc_now,
            is_pk_url_safe=False,
        )

        class UserError(Exception):
            pass

        with orm.Session(engine) as ses:
            try:
                with ses.begin():
                    task1 = Task(
                        url=url,
                        update_at=utc_now,
                        # this is a helper method that convert the put s3 results
                        # to INSERT / UPDATE values
                        **put_s3_result.to_values(),
                    )
                    # intentionally raises an error to simulate a database failure
                    raise UserError()
                    ses.add(task1)
            except Exception as e:
                # clean up created s3 object when create row failed
                # if you don't want to do that, just don't run this method
                put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

        assert ses.get(Task, url) is None
        values = put_s3_result.to_values()
        html_s3_uri = values["html"]
        image_s3_uri = values["image"]
        assert S3Path(html_s3_uri).exists() is False
        assert S3Path(image_s3_uri).exists() is False

        utc_now = get_utc_now()

        put_s3_result = aws_s3.put_s3(
            api_calls=[
                aws_s3.PutS3ApiCall(
                    column="html",
                    binary=html_content_1,
                    old_s3_uri=None,
                    extra_put_object_kwargs=html_additional_kwargs,
                ),
                aws_s3.PutS3ApiCall(
                    column="image",
                    binary=image_content_1,
                    old_s3_uri=None,
                    extra_put_object_kwargs=image_additional_kwargs,
                ),
            ],
            s3_client=bsm.s3_client,
            pk=url,
            bucket=s3dir_root.bucket,
            prefix=s3dir_root.key,
            update_at=utc_now,
            is_pk_url_safe=False,
        )

        with orm.Session(engine) as ses:
            try:
                with ses.begin():
                    task1 = Task(
                        url=url,
                        update_at=utc_now,
                        # this is a helper method that convert the put s3 results
                        # to INSERT / UPDATE values
                        **put_s3_result.to_values(),
                    )
                    ses.add(task1)
            except Exception as e:
                # clean up created s3 object when create row failed
                # if you don't want to do that, just don't run this method
                put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

        task1: Task = ses.get(Task, url)
        assert task1.url == url
        assert task1.update_at == utc_now
        assert S3Path(task1.html).read_bytes() == html_content_1
        assert S3Path(task1.image).read_bytes() == image_content_1

        html_content_2 = b"<html>this is html 2</html>"
        image_content_2 = b"this is image 2"
        utc_now = get_utc_now()

        put_s3_result = aws_s3.put_s3(
            api_calls=[
                aws_s3.PutS3ApiCall(
                    column="html",
                    binary=html_content_2,
                    # since this is an updates, you have to specify the old s3 object,
                    # even it is None. we need this information to clean up old s3 object
                    # when SQL UPDATE succeeded
                    old_s3_uri=task1.html,
                    extra_put_object_kwargs=html_additional_kwargs,
                ),
                aws_s3.PutS3ApiCall(
                    column="image",
                    binary=image_content_2,
                    # since this is an updates, you have to specify the old s3 object,
                    # even it is None. we need this information to clean up old s3 object
                    # when SQL UPDATE succeeded
                    old_s3_uri=task1.image,
                    extra_put_object_kwargs=image_additional_kwargs,
                ),
            ],
            s3_client=bsm.s3_client,
            pk=url,
            bucket=s3dir_root.bucket,
            prefix=s3dir_root.key,
            update_at=utc_now,
            is_pk_url_safe=False,
        )

        with orm.Session(engine) as ses:
            try:
                with ses.begin():
                    stmt = (
                        sa.update(Task).where(Task.url == url)
                        # this is a helper method that convert the put s3 results
                        # to INSERT / UPDATE values
                        .values(update_at=utc_now, **put_s3_result.to_values())
                    )
                    # intentionally raises an error to simulate a database failure
                    raise UserError()
                    ses.execute(stmt)
                # clean up old s3 object when update row succeeded
                # if you don't want to do that, just don't run this method
                put_s3_result.clean_up_old_s3_object_when_update_row_succeeded()
            except Exception as e:
                # clean up created s3 object when update row failed
                # if you don't want to do that, just don't run this method
                put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

        task2: Task = ses.get(Task, url)
        assert task2.update_at < utc_now
        assert S3Path(task1.html).read_bytes() == html_content_1
        assert S3Path(task1.image).read_bytes() == image_content_1
        values = put_s3_result.to_values()
        html_s3_uri = values["html"]
        image_s3_uri = values["image"]
        assert S3Path(html_s3_uri).exists() is False
        assert S3Path(image_s3_uri).exists() is False

        # ------------------------------------------------------------------------------
        # Update a Row and SQL UPDATE succeeded
        # ------------------------------------------------------------------------------
        utc_now = get_utc_now()

        put_s3_result = aws_s3.put_s3(
            api_calls=[
                aws_s3.PutS3ApiCall(
                    column="html",
                    binary=html_content_2,
                    # since this is an updates, you have to specify the old s3 object,
                    # even it is None. we need this information to clean up old s3 object
                    # when SQL UPDATE succeeded
                    old_s3_uri=task1.html,
                    extra_put_object_kwargs=html_additional_kwargs,
                ),
                aws_s3.PutS3ApiCall(
                    column="image",
                    binary=image_content_2,
                    # since this is an updates, you have to specify the old s3 object,
                    # even it is None. we need this information to clean up old s3 object
                    # when SQL UPDATE succeeded
                    old_s3_uri=task1.image,
                    extra_put_object_kwargs=image_additional_kwargs,
                ),
            ],
            s3_client=bsm.s3_client,
            pk=url,
            bucket=s3dir_root.bucket,
            prefix=s3dir_root.key,
            update_at=utc_now,
            is_pk_url_safe=False,
        )

        with orm.Session(engine) as ses:
            try:
                with ses.begin():
                    stmt = (
                        sa.update(Task).where(Task.url == url)
                        # this is a helper method that convert the put s3 results
                        # to INSERT / UPDATE values
                        .values(update_at=utc_now, **put_s3_result.to_values())
                    )
                    ses.execute(stmt)
                # clean up old s3 object when update row succeeded
                # if you don't want to do that, just don't run this method
                put_s3_result.clean_up_old_s3_object_when_update_row_succeeded()
            except Exception as e:
                # clean up created s3 object when update row failed
                # if you don't want to do that, just don't run this method
                put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

        task2: Task = ses.get(Task, url)
        assert task2.update_at == utc_now
        assert S3Path(task1.html).exists() is False
        assert S3Path(task1.image).exists() is False
        assert S3Path(task2.html).read_bytes() == html_content_2
        assert S3Path(task2.image).read_bytes() == image_content_2

        with orm.Session(engine) as ses:
            task3: Task = ses.get(Task, url)
            try:
                stmt = sa.delete(Task).where(Task.url == url)
                res = ses.execute(stmt)
                ses.commit()
                if res.rowcount == 1:
                    # clean up old s3 object when delete row succeeded
                    # if you don't want to do that, just don't run this method
                    if task3.html:
                        S3Path(task3.html).delete()
                    if task3.image:
                        S3Path(task3.image).delete()
            except Exception as e:
                ses.rollback()

        assert ses.get(Task, url) is None
        assert S3Path(task3.html).exists() is False
        assert S3Path(task3.image).exists() is False


class TestSqlite(BaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPsql(BaseTest):  # pragma: no cover
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_mate.patterns.large_binary_column.aws_s3",
        preview=False,
    )
