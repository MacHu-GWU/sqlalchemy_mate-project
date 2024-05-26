# -*- coding: utf-8 -*-

"""
一个用于展示如何正确实现 SQL DB 和 AWS S3 双写一致性的问题.
"""

from datetime import datetime, timezone

from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam
import sqlalchemy_mate.patterns.s3backed_column.api as s3backed_column
from rich import print as rprint
from rich.console import Console

aws_s3 = s3backed_column.aws_s3
console = Console()


def get_utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)


Base = orm.declarative_base()


class Task(Base, sam.ExtendedBase):
    __tablename__ = "tasks"

    _settings_major_attrs = ["url"]

    url = orm.mapped_column(sa.String, primary_key=True)
    update_at = orm.mapped_column(sa.DateTime)
    html = orm.mapped_column(sa.String, nullable=True)
    image = orm.mapped_column(sa.String, nullable=True)


engine = sa.create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)

bsm = BotoSesManager()
context.attach_boto_session(bsm.boto_ses)
bucket = f"{bsm.aws_account_alias}-{bsm.aws_region}-data"
s3dir_root = S3Path(
    f"s3://{bucket}/projects/sqlalchemy_mate/patterns/s3backed_column/data/"
).to_dir()
s3dir_root.delete()  # clean up everything to ensure a fresh start

url = "https://www.python.org"

# ------ Create row ------
console.rule("Create row", characters="=")
html_content = b"<html>this is html 1</html>"
image_content = b"this is image 1"
html_additional_kwargs = dict(ContentType="text/html")
image_additional_kwargs = dict(ContentType="image/jpeg")

utc_now = get_utc_now()
put_s3_result = aws_s3.put_s3(
    api_calls=[
        aws_s3.PutS3ApiCall(
            column="html",
            binary=html_content,
            old_s3_uri=None,
            extra_put_object_kwargs=html_additional_kwargs,
        ),
        aws_s3.PutS3ApiCall(
            column="image",
            binary=image_content,
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
                **put_s3_result.to_values(),
            )
            ses.add(task1)
        rprint("Succeeded!")
    except Exception as e:
        rprint("Failed!")
        put_s3_result.clean_up_created_s3_object_when_create_row_failed()
        raise

    console.rule("Row", characters="-")
    task1: Task = ses.get(Task, url)
    rprint(task1.to_dict())
    rprint(f"{S3Path(task1.html).read_text() = }")
    rprint(f"{S3Path(task1.image).read_bytes() = }")

# ------ Update row ------
console.rule("Update row", characters="=")
html_content = b"<html>this is html 2</html>"
image_content = b"this is image 2"

utc_now = get_utc_now()

put_s3_result = aws_s3.put_s3(
    api_calls=[
        aws_s3.PutS3ApiCall(
            column="html",
            binary=html_content,
            old_s3_uri=task1.html,
            extra_put_object_kwargs=html_additional_kwargs,
        ),
        aws_s3.PutS3ApiCall(
            column="image",
            binary=image_content,
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
                sa.update(Task)
                .where(Task.url == url)
                .values(**put_s3_result.to_values())
            )
            ses.execute(stmt)
        put_s3_result.clean_up_old_s3_object_when_update_row_succeeded()
        rprint("Succeeded!")
    except Exception as e:
        rprint("Failed!")
        put_s3_result.clean_up_created_s3_object_when_update_row_failed()
        raise e

    console.rule("Row", characters="-")
    task2: Task = ses.get(Task, url)
    rprint(task2.to_dict())
    rprint(f"{S3Path(task1.html).exists() = }")
    rprint(f"{S3Path(task1.image).exists() = }")
    rprint(f"{S3Path(task2.html).read_text() = }")
    rprint(f"{S3Path(task2.image).read_bytes() = }")
