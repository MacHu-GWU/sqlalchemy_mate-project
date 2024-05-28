# -*- coding: utf-8 -*-

"""
Use local file system as the storage backend.
"""

import typing as T
import os
import dataclasses
from pathlib import Path

from .helpers import get_md5, encode_pk, T_PK, execute_write


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
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


def clean_up_new_file_when_create_or_update_row_failed(
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

    def clean_up_new_file_when_create_or_update_row_failed(self):
        for res in self.write_file_backed_column_results:
            clean_up_new_file_when_create_or_update_row_failed(
                new_path=res.new_path, executed=res.executed
            )

    def clean_up_old_file_when_update_row_succeeded(self):
        for res in self.write_file_backed_column_results:
            clean_up_old_file_when_update_row_succeeded(
                old_path=res.old_path, executed=res.executed
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
