# -*- coding: utf-8 -*-

"""
一个用于展示如何正确实现 SQL DB 和本地文件双写一致性的问题.
"""

from datetime import datetime, timezone

import shutil
from pathlib import Path

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam
import sqlalchemy_mate.patterns.s3backed_column.api as s3backed_column
from rich import print as rprint
from rich.console import Console

local = s3backed_column.local
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

dir_root = Path(__file__).absolute().parent.joinpath("data")
shutil.rmtree(dir_root, ignore_errors=True)
dir_root.mkdir(parents=True, exist_ok=True)

url = "https://www.python.org"

# ------ Create row ------
console.rule("Create row", characters="=")
html_content = b"<html>this is html 1</html>"
image_content = b"this is image 1"

utc_now = get_utc_now()
write_file_result = local.write_file(
    api_calls=[
        local.WriteFileApiCall(
            column="html",
            binary=html_content,
            old_path=None,
        ),
        local.WriteFileApiCall(
            column="image",
            binary=image_content,
            old_path=None,
        ),
    ],
    pk=url,
    dir_root=dir_root,
    is_pk_path_safe=False,
)

with orm.Session(engine) as ses:
    try:
        with ses.begin():
            task1 = Task(
                url=url,
                update_at=utc_now,
                **write_file_result.to_values(),
            )
            ses.add(task1)
        rprint("Succeeded!")
    except Exception as e:
        rprint("Failed!")
        write_file_result.clean_up_new_file_when_create_row_failed()
        raise

    console.rule("Row", characters="-")
    task1: Task = ses.get(Task, url)
    rprint(task1.to_dict())
    rprint(f"{Path(task1.html).read_text() = }")
    rprint(f"{Path(task1.image).read_bytes() = }")

# ------ Update row ------
console.rule("Update row", characters="=")
html_content = b"<html>this is html 2</html>"
image_content = b"this is image 2"

utc_now = get_utc_now()
old_write_file_result = write_file_result
write_file_result = local.write_file(
    api_calls=[
        local.WriteFileApiCall(
            column="html",
            binary=html_content,
            old_path=Path(task1.html),
        ),
        local.WriteFileApiCall(
            column="image",
            binary=image_content,
            old_path=Path(task1.image),
        ),
    ],
    pk=url,
    dir_root=dir_root,
    is_pk_path_safe=False,
)

with orm.Session(engine) as ses:
    try:
        with ses.begin():
            stmt = (
                sa.update(Task)
                .where(Task.url == url)
                .values(**write_file_result.to_values())
            )
            ses.execute(stmt)
        write_file_result.clean_up_old_file_when_update_row_succeeded()
        rprint("Succeeded!")
    except Exception as e:
        rprint("Failed!")
        write_file_result.clean_up_new_s3_object_when_update_row_failed()
        raise e

    console.rule("Row", characters="-")
    task2: Task = ses.get(Task, url)
    rprint(task2.to_dict())
    rprint(f"{Path(task1.html).exists() = }")
    rprint(f"{Path(task1.image).exists() = }")
    rprint(f"{Path(task2.html).read_text() = }")
    rprint(f"{Path(task2.image).read_bytes() = }")
