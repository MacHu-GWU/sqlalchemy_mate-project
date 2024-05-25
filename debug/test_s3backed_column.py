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

console.rule("Create row", characters="=")
html_content = b"<html>this is html 1</html>"
image_content = b"this is image 1"

utc_now = get_utc_now()

actions = s3backed_column.put_s3(
    s3_client=bsm.s3_client,
    pk=url,
    kvs=dict(html=html_content, image=image_content),
    bucket=s3dir_root.bucket,
    prefix=s3dir_root.key,
    update_at=utc_now,
    is_pk_url_safe=False,
    s3_put_object_kwargs=dict(
        html=dict(ContentType="text/html"),
        image=dict(ContentType="image/jpeg"),
    ),
)

kwargs = {action.column: action.s3_uri for action in actions}
with orm.Session(engine) as ses:
    try:
        with ses.begin():
            task1 = Task(
                url=url,
                update_at=utc_now,
                **kwargs,
            )
            ses.add(task1)
        rprint("Succeeded!")
    except Exception as e:
        rprint("Failed!")
        s3backed_column.clean_up_created_s3_object_when_create_row_failed(
            s3_client=bsm.s3_client,
            actions=actions,
        )
        raise

    console.rule("Row", characters="-")
    task1: Task = ses.get(Task, url)
    rprint(task1.to_dict())
    rprint(f"{S3Path(task1.html).read_text() = }")
    rprint(f"{S3Path(task1.image).read_bytes() = }")

console.rule("Update row", characters="=")
html_content = b"<html>this is html 2</html>"
image_content = b"this is image 2"

utc_now = get_utc_now()

actions = s3backed_column.put_s3(
    s3_client=bsm.s3_client,
    pk=url,
    kvs=dict(html=html_content, image=image_content),
    bucket=s3dir_root.bucket,
    prefix=s3dir_root.key,
    update_at=utc_now,
    is_pk_url_safe=False,
    s3_put_object_kwargs=dict(
        html=dict(ContentType="text/html"),
        image=dict(ContentType="image/jpeg"),
    ),
)

kwargs = {action.column: action.s3_uri for action in actions}
with orm.Session(engine) as ses:
    try:
        with ses.begin():
            stmt = sa.update(Task).where(Task.url == url).values(**kwargs)
            ses.execute(stmt)
        s3backed_column.clean_up_old_s3_object_when_update_row_succeeded(
            s3_client=bsm.s3_client,
            actions=actions,
            old_kvs=dict(html=task1.html, image=task1.image),
        )
        rprint("Succeeded!")
    except Exception as e:
        rprint("Failed!")
        s3backed_column.clean_up_created_s3_object_when_update_row_failed(
            s3_client=bsm.s3_client,
            actions=actions,
        )
        raise e

    console.rule("Row", characters="-")
    task2: Task = ses.get(Task, url)
    rprint(task2.to_dict())
    rprint(f"{S3Path(task1.html).exists() = }")
    rprint(f"{S3Path(task1.image).exists() = }")
    rprint(f"{S3Path(task2.html).read_text() = }")
    rprint(f"{S3Path(task2.image).read_bytes() = }")
