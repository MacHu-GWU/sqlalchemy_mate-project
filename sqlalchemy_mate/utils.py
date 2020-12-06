# -*- coding: utf-8 -*-

"""
Utilities function.
"""

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

try:
    from typing import Type, Union, Tuple
except ImportError:  # pragma: no cover
    pass


def ensure_list(item):
    if not isinstance(item, (list, tuple)):
        return [item, ]
    else:
        return item


def grouper_list(l, n):
    """Evenly divide list into fixed-length piece, no filled value if chunk
    size smaller than fixed-length.

    Example::

        >>> list(grouper(range(10), n=3)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    **中文文档**

    将一个列表按照尺寸n, 依次打包输出, 有多少输出多少, 并不强制填充包的大小到n。

    下列实现是按照性能从高到低进行排列的:

    - 方法1: 建立一个counter, 在向chunk中添加元素时, 同时将counter与n比较, 如果一致
      则yield。然后在最后将剩余的item视情况yield。
    - 方法2: 建立一个list, 每次添加一个元素, 并检查size。
    - 方法3: 调用grouper()函数, 然后对里面的None元素进行清理。
    """
    chunk = list()
    counter = 0
    for item in l:
        counter += 1
        chunk.append(item)
        if counter == n:
            yield chunk
            chunk = list()
            counter = 0
    if len(chunk) > 0:
        yield chunk


session_klass_cache = dict()  # type: Dict[int, Type[Session]]


def ensure_session(engine_or_session):
    """
    If it is an engine, then create a session from it. And indicate that
    this session should be closed after the job done.

    :type engine_or_session: Union[Engine, Session]
    :rtype: Tuple[Session, bool]
    """
    if isinstance(engine_or_session, Engine):
        engine_id = id(engine_or_session)
        if id(engine_id) in session_klass_cache:  # pragma: no cover
            SessionClass = session_klass_cache[engine_id]
        else:  # pragma: no cover
            SessionClass = sessionmaker(bind=engine_or_session)
        ses = SessionClass()
        auto_close = True
        return ses, auto_close
    elif isinstance(engine_or_session, Session):
        ses = engine_or_session
        auto_close = False
        return ses, auto_close


def convert_query_to_sql_statement(query):
    """
    Convert a Query object created from orm query, into executable sql statement.

    :param query: :class:`sqlalchemy.orm.Query`

    :return: :class:`sqlalchemy.sql.selectable.Select`
    """
    context = query._compile_context()
    context.statement.use_labels = False
    return context.statement


def execute_query_return_result_proxy(query):
    """
    Execute a query, yield result proxy.

    :param query: :class:`sqlalchemy.orm.Query`,
        has to be created from ``session.query(Object)``

    :return: :class:`sqlalchemy.engine.result.ResultProxy`
    """
    context = query._compile_context()
    context.statement.use_labels = False
    if query._autoflush and not query._populate_existing:
        query.session._autoflush()

    conn = query._get_bind_args(
        context,
        query._connection_from_session,
        close_with_result=True)

    return conn.execute(context.statement, query._params)


from .pkg import timeout_decorator


def test_connection(engine, timeout=3):
    @timeout_decorator.timeout(timeout)
    def _test_connection(engine):
        v = engine.execute(sa.text("SELECT 1;")).fetchall()[0][0]
        assert v == 1
    try:
        _test_connection(engine)
        return True
    except timeout_decorator.TimeoutError:
        raise timeout_decorator.TimeoutError(
            "time out in %s seconds!" % timeout)
    except AssertionError:  # pragma: no cover
        raise ValueError
    except Exception as e:
        raise e
