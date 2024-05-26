# -*- coding: utf-8 -*-

import typing as T
import os
import dataclasses
from datetime import datetime
from pathlib import Path

import botocore.exceptions
from ...vendor.iterable import group_by
from .helpers import get_md5, b64encode_str, encode_pk, T_PK
from .storage import execute_write

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
# def split_s3_uri(s3_uri: str) -> T.Tuple[str, str]:
#     """
#     Split AWS S3 URI, returns bucket and key.
#     """
#     parts = s3_uri.split("/")
#     bucket = parts[2]
#     key = "/".join(parts[3:])
#     return bucket, key
#
#
# def join_s3_uri(bucket: str, key: str) -> str:
#     """
#     Join AWS S3 URI from bucket and key.
#     """
#     return "s3://{}/{}".format(bucket, key)
#
#
# def is_s3_object_exists(
#     s3_client: "S3Client",
#     bucket: str,
#     key: str,
# ) -> bool:  # pragma: no cover
#     try:
#         s3_client.head_object(Bucket=bucket, Key=key)
#         return True
#     except botocore.exceptions.ClientError as e:
#         if e.response["Error"]["Code"] == "404":
#             return False
#         else:  # pragma: no cover
#             raise e
#     except Exception as e:  # pragma: no cover
#         raise e
#
#
# def remove_s3_prefix(
#     s3_client: "S3Client",
#     bucket: str,
#     prefix: str,
# ):
#     """
#     Remove all objects with the same prefix in the bucket.
#     """
#     res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
#     for content in res.get("Contents", []):
#         key = content["Key"]
#         s3_client.delete_object(Bucket=bucket, Key=key)
#
#
# def batch_delete_s3_objects(
#     s3_client: "S3Client",
#     s3_uri_list: T.List[str],
# ):
#     """
#     Batch delete many S3 objects. If they share the same bucket, then use
#     the ``s3_client.delete_objects`` method. If they do not share the same bucket,
#     then use ``s3_client.delete_object`` method.
#
#     :param s3_client: ``boto3.client("s3")`` object.
#     :param s3_uri_list: example: ["s3://bucket/key1", "s3://bucket/key2"].
#     """
#     buckets = list()
#     keys = list()
#     pairs = list()
#     for s3_uri in s3_uri_list:
#         bucket, key = split_s3_uri(s3_uri)
#         pairs.append((bucket, key))
#
#         buckets.append(bucket)
#         keys.append(key)
#
#     groups = group_by(pairs, get_key=lambda x: x[0])
#     for bucket, bucket_key_pairs in groups.items():
#         s3_client.delete_objects(
#             Bucket=bucket,
#             Delete=dict(Objects=[dict(Key=key) for _, key in bucket_key_pairs]),
#         )
#
#
# def normalize_s3_prefix(prefix: str) -> str:
#     if prefix.startswith("/"):
#         prefix = prefix[1:]
#     elif prefix.endswith("/"):
#         prefix = prefix[:-1]
#     return prefix
#
#
def get_path(
    pk: str,
    column: str,
    binary: bytes,
    dir_root: Path,
) -> Path:
    """
    todo
    """
    md5 = get_md5(binary)
    return dir_root.joinpath(pk, f"col={column}", f"md5={md5}")


if os.name == "nt":
    path_sep = "\\"
elif os.name == "posix":
    path_sep = "/"
else:  # pragma: no cover
    raise NotImplementedError


# ------------------------------------------------------------------------------
# Low level API
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class WriteFileBackedColumnResult:
    # fmt: off
    column: str = dataclasses.field()
    old_path: Path = dataclasses.field()
    new_path: Path = dataclasses.field()
    executed: bool = dataclasses.field()
    cleanup_function: T.Callable = dataclasses.field()
    cleanup_old_kwargs: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default=None)
    cleanup_new_kwargs: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default=None)
    # fmt: on


def write_binary(
    path: Path,
    binary: bytes,
):
    try:
        path.write_bytes(binary),
    except FileNotFoundError:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(binary)


def write_file_backed_column(
    column: str,
    binary: bytes,
    old_path: T.Optional[Path],
    pk: T_PK,
    dir_root: Path,
    is_pk_path_safe: bool = False,
    extra_write_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
) -> WriteFileBackedColumnResult:
    # check_exists_function
    url_safe_pk = encode_pk(pk=pk, is_pk_url_safe=is_pk_path_safe, delimiter=path_sep)
    new_path = get_path(pk=url_safe_pk, column=column, binary=binary, dir_root=dir_root)
    check_exists_function = new_path.exists
    check_exists_kwargs = dict()
    # write_function
    if extra_write_kwargs is None:
        extra_write_kwargs = dict()
    write_function = write_binary
    write_kwargs = dict(path=new_path, binary=binary, **extra_write_kwargs)
    executed = execute_write(
        write_function=write_function,
        write_kwargs=write_kwargs,
        check_exists_function=check_exists_function,
        check_exists_kwargs=check_exists_kwargs,
    )
    # cleanup_function
    cleanup_function = new_path.unlink
    if old_path:
        cleanup_old_kwargs = dict()
    else:
        cleanup_old_kwargs = None
    cleanup_new_kwargs = dict()
    return WriteFileBackedColumnResult(
        column=column,
        old_path=old_path,
        new_path=new_path,
        executed=executed,
        cleanup_function=cleanup_function,
        cleanup_old_kwargs=cleanup_old_kwargs,
        cleanup_new_kwargs=cleanup_new_kwargs,
    )


def clean_up_new_file_when_create_row_failed(
    new_path: Path,
    executed: bool,
):
    """
    todo
    """
    if executed:
        new_path.unlink()


def clean_up_old_file_when_update_row_succeeded(
    old_path: T.Optional[Path],
    executed: bool,
):
    """
    todo
    """
    if executed:
        if old_path:
            old_path.unlink()


def clean_up_new_file_when_update_row_failed(
    new_path: Path,
    executed: bool,
):
    """
    todo
    """
    if executed:
        new_path.unlink()


# ------------------------------------------------------------------------------
# High Level API
# ------------------------------------------------------------------------------
@dataclasses.dataclass
class WriteFileApiCall:
    # fmt: off
    column: str = dataclasses.field()
    binary: bytes = dataclasses.field()
    old_path: T.Optional[Path] = dataclasses.field()
    extra_write_kwargs: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default_factory=dict)
    # fmt: on


@dataclasses.dataclass
class WriteFileResult:
    write_file_backed_column_results: T.List[WriteFileBackedColumnResult] = (
        dataclasses.field()
    )

    def to_values(self) -> T.Dict[str, str]:
        return {
            res.column: str(res.new_path)
            for res in self.write_file_backed_column_results
        }

    def clean_up_new_file_when_create_row_failed(self):
        for res in self.write_file_backed_column_results:
            clean_up_new_file_when_update_row_failed(
                new_path=res.new_path, executed=res.executed
            )

    def clean_up_old_file_when_update_row_succeeded(self):
        for res in self.write_file_backed_column_results:
            clean_up_old_file_when_update_row_succeeded(
                old_path=res.old_path, executed=res.executed
            )

    def clean_up_new_s3_object_when_update_row_failed(self):
        for res in self.write_file_backed_column_results:
            clean_up_new_file_when_update_row_failed(
                new_path=res.new_path, executed=res.executed
            )


def write_file(
    api_calls: T.List[WriteFileApiCall],
    pk: T_PK,
    dir_root: Path,
    is_pk_path_safe: bool = False,
):
    write_file_backed_column_results = list()
    for api_call in api_calls:
        write_file_backed_column_result = write_file_backed_column(
            column=api_call.column,
            binary=api_call.binary,
            old_path=api_call.old_path,
            pk=pk,
            dir_root=dir_root,
            is_pk_path_safe=is_pk_path_safe,
            extra_write_kwargs=api_call.extra_write_kwargs,
        )
        write_file_backed_column_results.append(write_file_backed_column_result)
    return WriteFileResult(
        write_file_backed_column_results=write_file_backed_column_results,
    )
