# -*- coding: utf-8 -*-

"""
"""

import typing as T
import hashlib
import dataclasses
from datetime import datetime

from . import helpers


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def get_md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def normalize_s3_prefix(prefix: str) -> str:
    if prefix.startswith("/"):
        prefix = prefix[1:]
    elif prefix.endswith("/"):
        prefix = prefix[:-1]
    return prefix


@dataclasses.dataclass
class Action:
    """
    表示一个 put_object 操作是否执行了, 以及执行的结果. 由于我们使用 content based hash
    作为 S3 URI 的一部分, 一旦 S3 object 已经存在, 我们是不会执行 s3_client.put_object 操作的.
    换言之, 一旦我们执行了, 那么这个 Column 的值肯定改变了 (换了一个 S3 URI).

    :param column: column name
    :param s3_uri: S3 object URI.
    :param put_executed: Whether the s3_client.put_object API call happened.
    """

    column: str
    s3_uri: str
    put_executed: bool


def get_s3_key(
    pk: str,
    column: str,
    value: bytes,
    prefix: str,
) -> str:
    """
    :param pk: primary key of the row. 注意这里的 pk 的值需要时 URL safe 的.
        因为它会作为 S3 key 的一部分. 如果你的 primary key 不是 URL safe 的,
        那么你需要用 ``helpers.b64encode_str(pk)`` 来转换. 但由于一般你不会
        手动调用这个函数, 所以你大概率不用担心这一问题.
    """
    prefix = normalize_s3_prefix(prefix)
    md5 = get_md5(value)
    return f"{prefix}/pk={pk}/col={column}/md5={md5}"


def put_s3(
    s3_client: "S3Client",
    pk: str,
    kvs: T.Dict[str, bytes],
    bucket: str,
    prefix: str,
    update_at: datetime,
    is_pk_url_safe: bool = False,
    s3_put_object_kwargs: T.Optional[T.Dict[str, T.Dict[str, T.Any]]] = None,
):
    """
    :param pk: primary key of the row. 这里的 pk 是逻辑意义上的, 如果你的 table
        是一个 compound primary key, 那么你需要把所有的 primary key 想办法拼接成一个字符串
        并且保证唯一性.
    :param is_pk_url_safe: 如果你的 pk 不是 url safe 的, 请设定这个参数为 False.
    """
    if s3_put_object_kwargs is None:
        s3_put_object_kwargs = dict()
    actions = list()
    if is_pk_url_safe:
        url_safe_pk = pk
    else:
        url_safe_pk = helpers.b64encode_str(pk)
    for column, value in kvs.items():
        s3_key = get_s3_key(pk=url_safe_pk, column=column, value=value, prefix=prefix)
        s3_uri = helpers.join_s3_uri(bucket, s3_key)
        if helpers.is_s3_object_exists(s3_client, bucket=bucket, key=s3_key):
            put_executed = False
        else:
            put_object_kwargs = s3_put_object_kwargs.get(column, {})
            metadata = put_object_kwargs.get("Metadata", {})
            metadata["pk"] = pk
            metadata["column"] = column
            metadata["update_at"] = update_at.isoformat()
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=value,
                Metadata=metadata,
                **put_object_kwargs,
            )
            put_executed = True
        actions.append(
            Action(
                column=column,
                s3_uri=s3_uri,
                put_executed=put_executed,
            )
        )
    return actions


def clean_up_created_s3_object_when_create_row_failed(
    s3_client: "S3Client",
    actions: T.List[Action],
):
    """
    Call this method to clean up when the ``session.add(...)``,
    then ``session.commit()`` operation failed.

    :param s3_client: ``boto3.client("s3")`` object.
    :param actions: list of :class:`Action` object.
    """
    s3_uri_list = list()
    for action in actions:
        if action.put_executed:
            s3_uri_list.append(action.s3_uri)
    helpers.batch_delete_s3_objects(s3_client, s3_uri_list)


def clean_up_old_s3_object_when_update_row_succeeded(
    s3_client: "S3Client",
    actions: T.List[Action],
    old_kvs: T.Dict[str, str],
):
    """
    Call this method to clean up when the sqlalchemy update operation succeeded.
    Because when you changed the value of the large attribute,
    you actually created a new S3 object. This method can clean up the old S3 object.

    :param s3_client: ``boto3.client("s3")`` object.
    :param actions: list of :class:`Action` object.
    :param old_kvs: the column value before updating it, we need this
        to figure out where to delete old S3 object.
    """
    s3_uri_list = list()
    for action in actions:
        # 当 put_executed 为 False 时, 说明, 我们并没有创建新的 object, 换言之旧的
        # object 依然有效, 所以我们不需要再 SQL update 成功时 clean up 旧的 object
        #
        # 而 put_executed 为 True 时, 所以我们一定是创建了新的 object 了, 那么
        # 有没有可能新的 uri 和 旧的 uri 相同呢? 这种情况下我们如果 clean up 旧的 object
        # 但实际上把新的 object 也删掉了, 这是不对的. 但是我认为这种事情不可能发生,
        # 因为如果新的 uri 和 旧的 uri 相同, 那么因为旧的 uri 存在则 S3 object 必然存在,
        # 这是由我们的一致性算法所保证的, 那么我们就不可能执行 put_object 操作. 所以我们没有
        # 必要检查新的 uri 和 旧的 uri 是否相同.
        if action.put_executed:
            # 删除旧的 S3 object
            s3_uri = old_kvs[action.column]
            # 如果之前 attr 的值是 None, 那么我们就不需要删除 S3 object
            if s3_uri:
                s3_uri_list.append(s3_uri)
    helpers.batch_delete_s3_objects(s3_client, s3_uri_list)


def clean_up_created_s3_object_when_update_row_failed(
    s3_client: "S3Client",
    actions: T.List[Action],
):
    """
    Call this method to clean up when the sqlalchemy update operation failed.
    Because you may have created a new S3 object, but since
    the SQL update operation failed, you don't need the new S3 object.
    This method can clean up the new S3 object.

    :param s3_client: ``boto3.client("s3")`` object.
    :param old_model: the old model object before updating it, we need this
        to figure out whether the old S3 object got changed.
    """
    s3_uri_list = list()
    for action in actions:
        # 当 put_executed 为 False 时, 说明, 我们并没有创建新的 object, 换言之旧的
        # object 依然有效, 所以我们不需要再 SQL update 失败时 clean up 新的 object
        #
        # 而 put_executed 为 True 时, 所以我们一定是创建了新的 object 了, 那么
        # 有没有可能新的 uri 和 旧的 uri 相同呢? 这种情况下我们如果 clean up 新的 object
        # 但实际上把旧的 object 也删掉了, 这是不对的. 但是我认为这种事情不可能发生,
        # 因为如果新的 uri 和 旧的 uri 相同, 那么因为旧的 uri 存在则 S3 object 必然存在,
        # 这是由我们的一致性算法所保证的, 那么我们就不可能执行 put_object 操作. 所以我们没有
        # 必要检查新的 uri 和 旧的 uri 是否相同.
        if action.put_executed:
            # 删除新的 S3 object
            s3_uri_list.append(action.s3_uri)
    helpers.batch_delete_s3_objects(s3_client, s3_uri_list)
