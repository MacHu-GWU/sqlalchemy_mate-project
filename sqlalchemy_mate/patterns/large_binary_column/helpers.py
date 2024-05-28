# -*- coding: utf-8 -*-

import typing as T
import base64
import hashlib


def get_md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def get_sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def b64encode_str(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("utf-8")


def b64decode_str(s: str) -> str:
    return base64.urlsafe_b64decode(s.encode("utf-8")).decode("utf-8")


T_SINGLE_PK = T.Union[str, int]
T_PK = T.Union[T_SINGLE_PK, T.Iterable[T_SINGLE_PK]]


def encode_pk(
    pk: T_PK,
    is_pk_url_safe: bool,
    delimiter: str = "/",
) -> str:
    """
    :param pk: primary key of the row. It could be a single value or a list of values
        (when pk is compound).
    :param is_pk_url_safe: whether the primary key is URL safe. If it's not, you need to
        encode it with b64encode.
    :param delimiter: the delimiter to join the primary key values.
    """
    if is_pk_url_safe:
        if isinstance(pk, str):
            return pk
        else:
            return delimiter.join(pk)
    else:
        if isinstance(pk, str):
            return b64encode_str(pk)
        else:
            return delimiter.join(b64encode_str(p) for p in pk)


def execute_write(
    write_function: T.Callable,
    write_kwargs: T.Dict[str, T.Any],
    check_exists_function: T.Optional[T.Callable] = None,
    check_exists_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
) -> bool:
    """
    :return: a boolean flag indicating whether the write operation is executed.
    """
    if check_exists_function is None:
        exists = False
    else:
        exists = check_exists_function(**check_exists_kwargs)
        if exists is False:
            write_function(**write_kwargs)
    executed = not exists
    return executed
