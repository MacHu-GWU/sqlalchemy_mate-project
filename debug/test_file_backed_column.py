# -*- coding: utf-8 -*-

"""
一个用于展示如何正确实现 SQL DB 和本地文件双写一致性的问题.
"""

import typing as T
from datetime import datetime

import shutil
from pathlib import Path

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

from rich import print as rprint
from rich import box
from rich.console import Console
from rich.panel import Panel

local = sam.patterns.large_binary_column.local
console = Console()


def get_utc_now() -> datetime:
    return datetime.utcnow()


Base = orm.declarative_base()


class Task(Base, sam.ExtendedBase):
    __tablename__ = "tasks"

    _settings_major_attrs = ["url"]

    url: orm.Mapped[str] = orm.mapped_column(sa.String, primary_key=True)
    update_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    html: orm.Mapped[T.Optional[str]] = orm.mapped_column(sa.String, nullable=True)
    image: orm.Mapped[T.Optional[str]] = orm.mapped_column(sa.String, nullable=True)


engine = sa.create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)

dir_root = Path(__file__).absolute().parent.joinpath("data")
shutil.rmtree(dir_root, ignore_errors=True)
dir_root.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Create a Row but SQL INSERT failed
# ------------------------------------------------------------------------------
______Create_a_Row_but_SQL_INSERT_failed = None
rprint(
    Panel(
        "Create a Row but SQL INSERT failed", box=box.DOUBLE, border_style="bold green"
    )
)

url = "https://www.example.com"

html_content_1 = b"<html>this is html 1</html>"
image_content_1 = b"this is image 1"
utc_now = get_utc_now()

rprint(Panel("Write file first, then write DB"))
write_file_result = local.write_file(
    api_calls=[
        local.WriteFileApiCall(
            column="html",
            binary=html_content_1,
            old_path=None,
        ),
        local.WriteFileApiCall(
            column="image",
            binary=image_content_1,
            old_path=None,
        ),
    ],
    pk=url,
    dir_root=dir_root,
    is_pk_path_safe=False,
)
rprint(write_file_result)


class UserError(Exception):
    pass


with orm.Session(engine) as ses:
    try:
        with ses.begin():
            task1 = Task(
                url=url,
                update_at=utc_now,
                # this is a helper method that convert the write file results
                # to INSERT / UPDATE values
                **write_file_result.to_values(),
            )
            # intentionally raises an error to simulate a database failure
            raise UserError()
            ses.add(task1)
        rprint("SQL INSERT Succeeded!")
    except Exception as e:
        rprint(f"SQL INSERT Failed! Error: {e!r}")
        # clean up created file when create row failed
        # if you don't want to do that, just don't run this method
        write_file_result.clean_up_new_file_when_create_or_update_row_failed()


rprint(Panel("Database row should not exists"))
rprint(f"{ses.get(Task, url) = }")
assert ses.get(Task, url) is None
rprint(Panel("file should be deleted"))
values = write_file_result.to_values()
html_path = values["html"]
image_path = values["image"]
rprint(f"{Path(html_path).exists() = }")
rprint(f"{Path(image_path).exists() = }")
assert Path(html_path).exists() is False
assert Path(image_path).exists() is False


# ------------------------------------------------------------------------------
# Create a Row and SQL INSERT succeeded
# ------------------------------------------------------------------------------
______Create_a_Row_and_SQL_INSERT_succeeded = None
rprint(
    Panel(
        "Create a Row and SQL INSERT succeeded",
        box=box.DOUBLE,
        border_style="bold green",
    )
)

utc_now = get_utc_now()

rprint(Panel("Write file first, then write DB"))
write_file_result = local.write_file(
    api_calls=[
        local.WriteFileApiCall(
            column="html",
            binary=html_content_1,
            old_path=None,
        ),
        local.WriteFileApiCall(
            column="image",
            binary=image_content_1,
            old_path=None,
        ),
    ],
    pk=url,
    dir_root=dir_root,
    is_pk_path_safe=False,
)
rprint(write_file_result)


class UserError(Exception):
    pass


with orm.Session(engine) as ses:
    try:
        with ses.begin():
            task1 = Task(
                url=url,
                update_at=utc_now,
                # this is a helper method that convert the write file results
                # to INSERT / UPDATE values
                **write_file_result.to_values(),
            )
            ses.add(task1)
        rprint("SQL INSERT Succeeded!")
    except Exception as e:
        rprint(f"SQL INSERT Failed! Error: {e!r}")
        # clean up created file when create row failed
        # if you don't want to do that, just don't run this method
        write_file_result.clean_up_new_file_when_create_or_update_row_failed()


rprint(Panel("Database row should not exists"))
task1: Task = ses.get(Task, url)
assert task1.url == url
assert task1.update_at == utc_now
rprint(Panel("file should be created"))
values = write_file_result.to_values()
html_path = values["html"]
image_path = values["image"]
rprint(f"{Path(html_path).read_bytes() = }")
rprint(f"{Path(image_path).read_bytes() = }")
assert Path(html_path).read_bytes() == html_content_1
assert Path(image_path).read_bytes() == image_content_1


# ------------------------------------------------------------------------------
# Update a Row but SQL UPDATE failed
# ------------------------------------------------------------------------------
______Update_a_Row_but_SQL_UPDATE_failed = None
rprint(
    Panel(
        "Update a Row but SQL UPDATE failed", box=box.DOUBLE, border_style="bold green"
    )
)

html_content_2 = b"<html>this is html 2</html>"
image_content_2 = b"this is image 2"
utc_now = get_utc_now()

rprint(Panel("Write S3 first, then write DB"))
write_file_result = local.write_file(
    api_calls=[
        local.WriteFileApiCall(
            column="html",
            binary=html_content_2,
            # since this is an updates, you have to specify the old file,
            # even it is None. we need this information to clean up old file
            # when SQL UPDATE succeeded
            old_path=Path(task1.html),
        ),
        local.WriteFileApiCall(
            column="image",
            binary=image_content_2,
            # since this is an updates, you have to specify the old file,
            # even it is None. we need this information to clean up old file
            # when SQL UPDATE succeeded
            old_path=Path(task1.image),
        ),
    ],
    pk=url,
    dir_root=dir_root,
    is_pk_path_safe=False,
)
rprint(write_file_result)

with orm.Session(engine) as ses:
    try:
        with ses.begin():
            stmt = (
                sa.update(Task)
                .where(Task.url == url)
                .values(update_at=utc_now, **write_file_result.to_values())
            )
            # intentionally raises an error to simulate a database failure
            raise UserError()
            ses.execute(stmt)
        print("SQL UPDATE Succeeded!")
        # clean up file when update row succeeded
        # if you don't want to do that, just don't run this method
        write_file_result.clean_up_old_file_when_update_row_succeeded()
    except Exception as e:
        rprint(f"SQL UPDATE Failed! Error: {e!r}")
        # clean up created file when update row failed
        # if you don't want to do that, just don't run this method
        write_file_result.clean_up_new_file_when_create_or_update_row_failed()

rprint(Panel("Database row should not be updated"))
task2: Task = ses.get(Task, url)
rprint(task2.__dict__)
assert task2.update_at < utc_now
rprint(Panel("Old file should still be there"))
rprint(f"{Path(task1.html).read_bytes() = }")
rprint(f"{Path(task1.image).read_bytes() = }")
assert Path(task1.html).read_bytes() == html_content_1
assert Path(task1.image).read_bytes() == image_content_1
rprint(Panel("New file should be deleted"))
values = write_file_result.to_values()
html_path = values["html"]
image_path = values["image"]
rprint(f"{Path(html_path).exists() = }")
rprint(f"{Path(image_path).exists() = }")
assert Path(html_path).exists() is False
assert Path(image_path).exists() is False


# ------------------------------------------------------------------------------
# Update a Row and SQL UPDATE succeeded
# ------------------------------------------------------------------------------
______Update_a_Row_and_SQL_UPDATE_succeeded = None
rprint(
    Panel(
        "Update a Row and SQL UPDATE succeeded",
        box=box.DOUBLE,
        border_style="bold green",
    )
)
utc_now = get_utc_now()

rprint(Panel("Write S3 first, then write DB"))
write_file_result = local.write_file(
    api_calls=[
        local.WriteFileApiCall(
            column="html",
            binary=html_content_2,
            # since this is an updates, you have to specify the old file,
            # even it is None. we need this information to clean up old file
            # when SQL UPDATE succeeded
            old_path=Path(task1.html),
        ),
        local.WriteFileApiCall(
            column="image",
            binary=image_content_2,
            # since this is an updates, you have to specify the old file,
            # even it is None. we need this information to clean up old file
            # when SQL UPDATE succeeded
            old_path=Path(task1.image),
        ),
    ],
    pk=url,
    dir_root=dir_root,
    is_pk_path_safe=False,
)
rprint(write_file_result)

with orm.Session(engine) as ses:
    try:
        with ses.begin():
            stmt = (
                sa.update(Task)
                .where(Task.url == url)
                .values(update_at=utc_now, **write_file_result.to_values())
            )
            ses.execute(stmt)
        print("SQL UPDATE Succeeded!")
        # clean up file when update row succeeded
        # if you don't want to do that, just don't run this method
        write_file_result.clean_up_old_file_when_update_row_succeeded()
    except Exception as e:
        rprint(f"SQL UPDATE Failed! Error: {e!r}")
        # clean up created file when update row failed
        # if you don't want to do that, just don't run this method
        write_file_result.clean_up_new_file_when_create_or_update_row_failed()

rprint(Panel("Database row should be updated"))
task2: Task = ses.get(Task, url)
rprint(task2.__dict__)
assert task2.update_at == utc_now
rprint(Panel("Old file should be deleted"))
rprint(f"{Path(task1.html).exists() = }")
rprint(f"{Path(task1.image).exists() = }")
assert Path(task1.html).exists() is False
assert Path(task1.image).exists() is False
rprint(Panel("New file should be created"))
rprint(f"{Path(task2.html).read_bytes() = }")
rprint(f"{Path(task2.image).read_bytes() = }")
assert Path(task2.html).read_bytes() == html_content_2
assert Path(task2.image).read_bytes() == image_content_2

# ------------------------------------------------------------------------------
# Delete a Row and SQL DELETE succeeded
# ------------------------------------------------------------------------------
______Delete_a_Row_and_SQL_DELETE_succeeded = None
rprint(
    Panel(
        "Delete a Row and SQL DELETE succeeded",
        box=box.DOUBLE,
        border_style="bold green",
    )
)

rprint(Panel("Delete DB first, then delete S3"))

with orm.Session(engine) as ses:
    task3: Task = ses.get(Task, url)
    try:
        stmt = sa.delete(Task).where(Task.url == url)
        res = ses.execute(stmt)
        ses.commit()
        if res.rowcount == 1:
            print("SQL DELETE Succeeded!")
            # clean up old s3 object when delete row succeeded
            # if you don't want to do that, just don't run this method
            if task3.html:
                Path(task3.html).unlink()
            if task3.image:
                Path(task3.image).unlink()
        else:
            print("SQL DELETE Failed! No row affected.")
    except Exception as e:
        ses.rollback()
        rprint(f"SQL DELETE Failed! Error: {e!r}")

rprint(Panel("Database row should be deleted"))
rprint(f"{ses.get(Task, url) = }")
assert ses.get(Task, url) is None
rprint(Panel("Old S3 object should be deleted"))
rprint(f"{Path(task3.html).exists() = }")
rprint(f"{Path(task3.image).exists() = }")
assert Path(task3.html).exists() is False
assert Path(task3.image).exists() is False
