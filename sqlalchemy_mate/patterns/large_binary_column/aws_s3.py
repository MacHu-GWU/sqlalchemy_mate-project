# -*- coding: utf-8 -*-

"""
Use Amazon S3 as the storage backend.
"""

import typing as T
import dataclasses
from datetime import datetime

import botocore.exceptions
from ...vendor.iterable import group_by

from .helpers import get_md5, encode_pk, T_PK, execute_write

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def split_s3_uri(s3_uri: str) -> T.Tuple[str, str]:
    """
    Split AWS S3 URI, returns bucket and key.
    """
    parts = s3_uri.split("/")
    bucket = parts[2]
    key = "/".join(parts[3:])
    return bucket, key


def join_s3_uri(bucket: str, key: str) -> str:
    """
    Join AWS S3 URI from bucket and key.
    """
    return "s3://{}/{}".format(bucket, key)


def is_s3_object_exists(
    s3_client: "S3Client",
    bucket: str,
    key: str,
) -> bool:  # pragma: no cover
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:  # pragma: no cover
            raise e
    except Exception as e:  # pragma: no cover
        raise e


def batch_delete_s3_objects(
    s3_client: "S3Client",
    s3_uri_list: T.List[str],
):
    """
    Batch delete many S3 objects. If they share the same bucket, then use
    the ``s3_client.delete_objects`` method. If they do not share the same bucket,
    then use ``s3_client.delete_object`` method.

    :param s3_client: ``boto3.client("s3")`` object.
    :param s3_uri_list: example: ["s3://bucket/key1", "s3://bucket/key2"].
    """
    buckets = list()
    keys = list()
    pairs = list()
    for s3_uri in s3_uri_list:
        bucket, key = split_s3_uri(s3_uri)
        pairs.append((bucket, key))

        buckets.append(bucket)
        keys.append(key)

    groups = group_by(pairs, get_key=lambda x: x[0])
    for bucket, bucket_key_pairs in groups.items():
        s3_client.delete_objects(
            Bucket=bucket,
            Delete=dict(Objects=[dict(Key=key) for _, key in bucket_key_pairs]),
        )


def normalize_s3_prefix(prefix: str) -> str:
    if prefix.startswith("/"):  # pragma: no cover
        prefix = prefix[1:]
    elif prefix.endswith("/"):
        prefix = prefix[:-1]
    return prefix


def get_s3_key(
    pk: str,
    column: str,
    binary: bytes,
    prefix: str,
) -> str:
    """
    todo
    """
    prefix = normalize_s3_prefix(prefix)
    md5 = get_md5(binary)
    return f"{prefix}/{pk}/col={column}/md5={md5}"


# ------------------------------------------------------------------------------
# Low level API
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class PutS3BackedColumnResult:
    """
    The returned object of :func:`put_s3backed_column`.

    :param column: which column is about to be created/updated.
    :param old_s3_uri: the old S3 URI, if it is a "create", then it is None.
    :param new_s3_uri:
    :param executed:
    :param cleanup_function:
    :param cleanup_old_kwargs:
    :param cleanup_new_kwargs:
    """

    # fmt: off
    column: str = dataclasses.field()
    old_s3_uri: str = dataclasses.field()
    new_s3_uri: str = dataclasses.field()
    executed: bool = dataclasses.field()
    cleanup_function: T.Callable = dataclasses.field()
    cleanup_old_kwargs: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default=None)
    cleanup_new_kwargs: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default=None)
    # fmt: on


def put_s3backed_column(
    column: str,
    binary: bytes,
    old_s3_uri: T.Optional[str],
    s3_client: "S3Client",
    pk: T_PK,
    bucket: str,
    prefix: str,
    update_at: datetime,
    is_pk_url_safe: bool = False,
    extra_put_object_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
) -> PutS3BackedColumnResult:
    """
    Put the binary data of a column to S3.

    :param column: which column is about to be created/updated.
    :param binary: the binary data of the column to be written to S3.
    :param old_s3_uri: if it is a "create row", then it is None.
        if it is a "update row", then it is the old value of the column (could be None).
    :param s3_client: ``boto3.client("s3")`` object.
    :param pk: the primary key of the row. It is used to generate the S3 key.
        it could be a single value or a tuple of values when primary key is a
        compound key.
    :param bucket: the S3 bucket name where you store the binary data.
    :param prefix: the common prefix of the S3 key. the prefix and the pk together
        will form the S3 key.
    :param update_at: logical timestamp of the "create row" or "update row",
        it will be written to the S3 object's metadata.
    :param is_pk_url_safe: whether the primary key is URL safe or not. If the
        primary key has special character, then you should set it to True.
        Set to False only if you are sure that the primary key is URL safe.
    :param extra_put_object_kwargs: additional custom keyword arguments for
        ``s3_client.put_object()`` API.
    """
    # check_exists_function
    url_safe_pk = encode_pk(pk=pk, is_pk_url_safe=is_pk_url_safe, delimiter="/")
    s3_key = get_s3_key(pk=url_safe_pk, column=column, binary=binary, prefix=prefix)
    new_s3_uri = join_s3_uri(bucket=bucket, key=s3_key)
    check_exists_function = is_s3_object_exists
    check_exists_kwargs = dict(
        s3_client=s3_client,
        bucket=bucket,
        key=s3_key,
    )
    # write_function
    if extra_put_object_kwargs is None:  # pragma: no cover
        extra_put_object_kwargs = dict()
    metadata = {"update_at": update_at.isoformat()}
    try:
        metadata.update(extra_put_object_kwargs.pop("Metadata"))
    except KeyError:  # pragma: no cover
        pass
    write_function = s3_client.put_object
    write_kwargs = dict(
        Bucket=bucket,
        Key=s3_key,
        Body=binary,
        Metadata=metadata,
        **extra_put_object_kwargs,
    )
    executed = execute_write(
        write_function=write_function,
        write_kwargs=write_kwargs,
        check_exists_function=check_exists_function,
        check_exists_kwargs=check_exists_kwargs,
    )
    # cleanup_function
    cleanup_function = s3_client.delete_object
    if old_s3_uri:
        _bucket, _s3_key = split_s3_uri(old_s3_uri)
        cleanup_old_kwargs = dict(Bucket=_bucket, Key=_s3_key)
    else:
        cleanup_old_kwargs = None
    cleanup_new_kwargs = dict(Bucket=bucket, Key=s3_key)
    return PutS3BackedColumnResult(
        column=column,
        old_s3_uri=old_s3_uri,
        new_s3_uri=new_s3_uri,
        executed=executed,
        cleanup_function=cleanup_function,
        cleanup_old_kwargs=cleanup_old_kwargs,
        cleanup_new_kwargs=cleanup_new_kwargs,
    )


def clean_up_created_s3_object_when_create_or_update_row_failed(
    s3_client: "S3Client",
    new_s3_uri: str,
    executed: bool,
):  # pragma: no cover
    """
    After ``s3_client.put_object()``, we need to create / update the row.
    If the create / update row failed, we may need to clean up the created S3 object.
    to ensure data consistency between S3 and database.

    :param s3_client: ``boto3.client("s3")`` object.
    :param new_s3_uri: the new S3 URI.
    :param executed: whether the ``s3_client.put_object()`` is executed.
    """
    if executed:
        bucket, key = split_s3_uri(new_s3_uri)
        s3_client.delete_object(Bucket=bucket, Key=key)


def clean_up_old_s3_object_when_update_row_succeeded(
    s3_client: "S3Client",
    old_s3_uri: T.Optional[str],
    executed: bool,
):  # pragma: no cover
    """
    Let's say after ``s3_client.put_object()``, we need to update the row.
    If the update row failed, we may need to clean up the old S3 object.

    :param s3_client: ``boto3.client("s3")`` object.
    :param old_s3_uri: the old S3 URI.
    :param executed: whether the ``s3_client.put_object()`` is executed.
    """
    if executed:
        if old_s3_uri:
            bucket, key = split_s3_uri(old_s3_uri)
            s3_client.delete_object(Bucket=bucket, Key=key)


# ------------------------------------------------------------------------------
# High Level API
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class PutS3ApiCall:
    """
    A data container of the arguments that will be used in ``s3_client.put_object()``.

    :param column: which column is about to be created/updated.
    :param binary: the binary data of the column to be written to S3.
    :param old_s3_uri: if it is a "create row", then it is None.
        if it is a "update row", then it is the old value of the column (could be None).
    :param extra_put_object_kwargs: additional custom keyword arguments for
        ``s3_client.put_object()`` API.
    """

    # fmt: off
    column: str = dataclasses.field()
    binary: bytes = dataclasses.field()
    old_s3_uri: T.Optional[str] = dataclasses.field()
    extra_put_object_kwargs: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default_factory=dict)
    # fmt: on


@dataclasses.dataclass
class PutS3Result:
    """
    The returned object of :func:`put_s3_result`.
    """

    s3_client: "S3Client" = dataclasses.field()
    put_s3backed_column_results: T.List[PutS3BackedColumnResult] = dataclasses.field()

    def to_values(self) -> T.Dict[str, str]:
        """
        Return a dictionary of column name and S3 uri that can be used in the
        SQL ``UPDATE ... VALUES ...`` statement. The key is the column name,
        and the value is the S3 URI.
        """
        return {res.column: res.new_s3_uri for res in self.put_s3backed_column_results}

    def clean_up_created_s3_object_when_create_or_update_row_failed(self):
        """
        A wrapper of :func:`clean_up_created_s3_object_when_create_or_update_row_failed`.
        """
        s3_uri_list = list()
        for res in self.put_s3backed_column_results:
            if res.executed:
                s3_uri_list.append(res.new_s3_uri)
        batch_delete_s3_objects(s3_client=self.s3_client, s3_uri_list=s3_uri_list)

    def clean_up_old_s3_object_when_update_row_succeeded(self):
        """
        A wrapper of :func:`clean_up_old_s3_object_when_update_row_succeeded`.
        """
        s3_uri_list = list()
        for res in self.put_s3backed_column_results:
            if res.executed:
                if res.old_s3_uri:
                    s3_uri_list.append(res.old_s3_uri)
        batch_delete_s3_objects(s3_client=self.s3_client, s3_uri_list=s3_uri_list)


def put_s3(
    api_calls: T.List[PutS3ApiCall],
    s3_client: "S3Client",
    pk: T_PK,
    bucket: str,
    prefix: str,
    update_at: datetime,
    is_pk_url_safe: bool = False,
):
    """
    Put the binary data of a column to S3.

    :param api_calls: a list of :class:`PutS3ApiCall` objects. It defines how to
        put the binary data of multiple columns to S3.
    :param s3_client: ``boto3.client("s3")`` object.
    :param pk: the primary key of the row. It is used to generate the S3 key.
        it could be a single value or a tuple of values when primary key is a
        compound key.
    :param bucket: the S3 bucket name where you store the binary data.
    :param prefix: the common prefix of the S3 key. the prefix and the pk together
        will form the S3 key.
    :param update_at: logical timestamp of the "create row" or "update row",
        it will be written to the S3 object's metadata.
    :param is_pk_url_safe: whether the primary key is URL safe or not. If the
        primary key has special character, then you should set it to True.
        Set to False only if you are sure that the primary key is URL safe.
    """
    put_s3backed_column_results = list()
    for api_call in api_calls:
        put_s3backed_column_result = put_s3backed_column(
            column=api_call.column,
            binary=api_call.binary,
            old_s3_uri=api_call.old_s3_uri,
            s3_client=s3_client,
            pk=pk,
            bucket=bucket,
            prefix=prefix,
            update_at=update_at,
            is_pk_url_safe=is_pk_url_safe,
            extra_put_object_kwargs=api_call.extra_put_object_kwargs,
        )
        put_s3backed_column_results.append(put_s3backed_column_result)
    return PutS3Result(
        s3_client=s3_client,
        put_s3backed_column_results=put_s3backed_column_results,
    )
