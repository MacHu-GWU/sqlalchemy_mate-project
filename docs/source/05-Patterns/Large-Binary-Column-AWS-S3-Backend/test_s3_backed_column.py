# -*- coding: utf-8 -*-

"""
一个用于展示如何正确实现 SQL DB 和 AWS S3 双写一致性的问题的例子.

Storing large binary data directly in a relational database can lead to performance and scalability issues. As the size of the binary data grows, it consumes valuable database disk space and increases the I/O overhead, potentially impacting query performance and overall database efficiency. To address this challenge, the recommended best practice is to employ a pattern that leverages external storage backends for storing large binary data while maintaining a reference to the data's location within the database.
By storing only a unique resource identifier (URI) as a column in the relational database, the actual binary data is offloaded to a dedicated storage layer. This approach allows for better utilization of database resources, as the database focuses on storing structured data and efficient querying. The external storage backend, such as a file system or cloud storage like Amazon S3, is optimized for handling large binary objects, providing scalability and cost-effectiveness.

**Example**

In this comprehensive example, we aim to demonstrate the complete lifecycle management of large binary data using the pattern that leverages an external storage system like **Amazon S3** in conjunction with a relational database. The example covers various scenarios to showcase the proper handling of data consistency and integrity.

1. In the first scenario, we attempt to **create a new row** in the database with a column containing a reference to the large binary data stored in S3. However, if the SQL ``INSERT`` operation fails unexpectedly, it is crucial to maintain data consistency by removing the orphaned S3 object (optionally, your choice). This ensures that there are no dangling references or unused data in the external storage.
2. The second scenario illustrates a successful **creation of a row** with a large binary data column. Here, we can observe how the binary data is efficiently stored in S3 and the corresponding reference is inserted into the database column.
3. In the third scenario, we try to **update the value of a large binary column in an existing row**. If the SQL ``UPDATE`` operation fails, it is essential to maintain the integrity of the data. We can see that the old S3 object remains unchanged, and the new S3 object, if created, is removed (optionally, your choice) to keep the system in a consistent state.
4. The fourth scenario demonstrates a successful **update of a large binary column value**. In this case, we can observe how the old S3 object is deleted (optionally, your choice) to free up storage space, and the new S3 object is created to reflect the updated binary data. This ensures that the database and S3 remain in sync.
5. Finally, the fifth scenario showcases the **deletion of a row** containing a large binary column. When a row is deleted from the database, it is important to clean up (optionally, your choice) the associated S3 object as well. By removing the corresponding S3 object, we maintain data consistency and prevent any orphaned binary data from lingering in the external storage.

**Conclusion**

By leveraging the capabilities of sqlalchemy_mate, developers can build scalable and efficient systems that handle large binary data with ease. The module's Pythonic interface, flexibility in storage backends, and extensibility make it a powerful tool for managing the lifecycle of large binary objects while ensuring data consistency and integrity.
"""

import typing as T
from datetime import datetime

from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

from rich import print as rprint
from rich import box
from rich.console import Console
from rich.panel import Panel

aws_s3 = sam.patterns.large_binary_column.aws_s3
console = Console()


def get_utc_now() -> datetime:
    return datetime.utcnow()


Base = orm.declarative_base()


class Task(Base):
    __tablename__ = "tasks"

    url: orm.Mapped[str] = orm.mapped_column(sa.String, primary_key=True)
    update_at: orm.Mapped[datetime] = orm.mapped_column(sa.DateTime)
    html: orm.Mapped[T.Optional[str]] = orm.mapped_column(sa.String, nullable=True)
    image: orm.Mapped[T.Optional[str]] = orm.mapped_column(sa.String, nullable=True)


engine = sa.create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)

bsm = BotoSesManager()
context.attach_boto_session(bsm.boto_ses)
bucket = f"{bsm.aws_account_alias}-{bsm.aws_region}-data"
s3dir_root = S3Path(
    f"s3://{bucket}/projects/sqlalchemy_mate/patterns/s3backed_column/data/"
).to_dir()

# clean up everything in database and s3 to ensure a fresh start
with engine.connect() as conn:
    conn.execute(Task.__table__.delete())
    conn.commit()
s3dir_root.delete()


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
html_additional_kwargs = dict(ContentType="text/html")
image_additional_kwargs = dict(ContentType="image/jpeg")
utc_now = get_utc_now()

rprint(Panel("Write S3 first, then write DB"))
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
rprint(put_s3_result)


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
        rprint("SQL INSERT Succeeded!")
    except Exception as e:
        rprint(f"SQL INSERT Failed! Error: {e!r}")
        # clean up created s3 object when create row failed
        # if you don't want to do that, just don't run this method
        put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

rprint(Panel("Database row should not exists"))
rprint(f"{ses.get(Task, url) = }")
assert ses.get(Task, url) is None
rprint(Panel("S3 object should be deleted"))
values = put_s3_result.to_values()
html_s3_uri = values["html"]
image_s3_uri = values["image"]
rprint(f"{S3Path(html_s3_uri).exists() = }")
rprint(f"{S3Path(image_s3_uri).exists() = }")
assert S3Path(html_s3_uri).exists() is False
assert S3Path(image_s3_uri).exists() is False

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

rprint(Panel("Write S3 first, then write DB"))
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
rprint(put_s3_result)

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
        rprint("SQL INSERT Succeeded!")
    except Exception as e:
        rprint(f"SQL INSERT Failed, error: {e!r}")
        # clean up created s3 object when create row failed
        # if you don't want to do that, just don't run this method
        put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

rprint(Panel("Database row should be inserted"))
task1: Task = ses.get(Task, url)
rprint(task1.__dict__)
assert task1.url == url
assert task1.update_at == utc_now
rprint(Panel("S3 object should be created"))
rprint(f"{S3Path(task1.html).read_bytes() = }")
rprint(f"{S3Path(task1.image).read_bytes() = }")
assert S3Path(task1.html).read_bytes() == html_content_1
assert S3Path(task1.image).read_bytes() == image_content_1

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
rprint(put_s3_result)

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
        print("SQL UPDATE Succeeded!")
        # clean up old s3 object when update row succeeded
        # if you don't want to do that, just don't run this method
        put_s3_result.clean_up_old_s3_object_when_update_row_succeeded()
    except Exception as e:
        rprint(f"SQL UPDATE Failed! Error: {e!r}")
        # clean up created s3 object when update row failed
        # if you don't want to do that, just don't run this method
        put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

rprint(Panel("Database row should not be updated"))
task2: Task = ses.get(Task, url)
rprint(task2.__dict__)
assert task2.update_at < utc_now
rprint(Panel("Old S3 object should still be there"))
rprint(f"{S3Path(task1.html).read_bytes() = }")
rprint(f"{S3Path(task1.image).read_bytes() = }")
assert S3Path(task1.html).read_bytes() == html_content_1
assert S3Path(task1.image).read_bytes() == image_content_1
rprint(Panel("New S3 object should be deleted"))
values = put_s3_result.to_values()
html_s3_uri = values["html"]
image_s3_uri = values["image"]
rprint(f"{S3Path(html_s3_uri).exists() = }")
rprint(f"{S3Path(image_s3_uri).exists() = }")
assert S3Path(html_s3_uri).exists() is False
assert S3Path(image_s3_uri).exists() is False

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
rprint(put_s3_result)

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
        print("SQL UPDATE Succeeded!")
        # clean up old s3 object when update row succeeded
        # if you don't want to do that, just don't run this method
        put_s3_result.clean_up_old_s3_object_when_update_row_succeeded()
    except Exception as e:
        rprint(f"SQL UPDATE Failed! Error: {e!r}")
        # clean up created s3 object when update row failed
        # if you don't want to do that, just don't run this method
        put_s3_result.clean_up_created_s3_object_when_create_or_update_row_failed()

rprint(Panel("Database row should be updated"))
task2: Task = ses.get(Task, url)
rprint(task2.__dict__)
assert task2.update_at == utc_now
rprint(Panel("Old S3 object should be deleted"))
rprint(f"{S3Path(task1.html).exists() = }")
rprint(f"{S3Path(task1.image).exists() = }")
assert S3Path(task1.html).exists() is False
assert S3Path(task1.image).exists() is False
rprint(Panel("New S3 object should be created"))
rprint(f"{S3Path(task2.html).read_bytes() = }")
rprint(f"{S3Path(task2.image).read_bytes() = }")
assert S3Path(task2.html).read_bytes() == html_content_2
assert S3Path(task2.image).read_bytes() == image_content_2


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
                S3Path(task3.html).delete()
            if task3.image:
                S3Path(task3.image).delete()
        else:
            print("SQL DELETE Failed! No row affected.")
    except Exception as e:
        ses.rollback()
        rprint(f"SQL DELETE Failed! Error: {e!r}")

rprint(Panel("Database row should be deleted"))
rprint(f"{ses.get(Task, url) = }")
assert ses.get(Task, url) is None
rprint(Panel("Old S3 object should be deleted"))
rprint(f"{S3Path(task3.html).exists() = }")
rprint(f"{S3Path(task3.image).exists() = }")
assert S3Path(task3.html).exists() is False
assert S3Path(task3.image).exists() is False
