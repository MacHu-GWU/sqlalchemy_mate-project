# -*- coding: utf-8 -*-

"""
This module provide utility functions for insert operation.
"""

import typing as T
import math

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from ..utils import grouper_list


def smart_insert(
    engine: sa.Engine,
    table: sa.Table,
    data: T.Union[dict, T.List[dict]],
    minimal_size: int = 5,
    _connection: sa.Connection = None,
    _op_counter: int = 0,
    _ins_counter: int = 0,
    _is_first_call: bool = True,
) -> T.Tuple[int, int]:
    """
    An optimized Insert strategy. Guarantee successful and highest insertion
    speed. But ATOMIC WRITE IS NOT ENSURED IF THE PROGRAM IS INTERRUPTED.

    :return: number of successful INSERT sql execution; number of inserted rows.

    **中文文档**

    在Insert中, 如果已经预知不会出现IntegrityError, 那么使用Bulk Insert的速度要
    远远快于逐条Insert。而如果无法预知, 那么我们采用如下策略:

    1. 尝试Bulk Insert, Bulk Insert由于在结束前不Commit, 所以速度很快。
    2. 如果失败了, 那么对数据的条数开平方根, 进行分包, 然后对每个包重复该逻辑。
    3. 若还是尝试失败, 则继续分包, 当分包的大小小于一定数量时, 则使用逐条插入。
      直到成功为止。

    该Insert策略在内存上需要额外的 sqrt(nbytes) 的开销, 跟原数据相比体积很小。
    但时间上是各种情况下平均最优的。
    """
    if _connection is None:
        _connection = engine.connect()

    insert = table.insert()

    if isinstance(data, list):
        # 首先进行尝试 bulk insert
        try:
            _connection.execute(insert, data)
            _connection.commit()
            _op_counter += 1
            _ins_counter += len(data)
        # 失败了
        except IntegrityError:
            _connection.rollback()
            # 分析数据量
            n = len(data)
            # 如果数据条数多于一定数量
            if n >= minimal_size**2:
                # 则进行分包
                n_chunk = math.floor(math.sqrt(n))
                for chunk in grouper_list(data, n_chunk):
                    _op_counter, _ins_counter = smart_insert(
                        engine=engine,
                        table=table,
                        data=chunk,
                        minimal_size=minimal_size,
                        _connection=_connection,
                        _op_counter=_op_counter,
                        _ins_counter=_ins_counter,
                        _is_first_call=False,
                    )
            # 否则则一条条地逐条插入
            else:
                for row in data:
                    try:
                        _connection.execute(insert.values(**row))
                        _connection.commit()
                        _op_counter += 1
                        _ins_counter += 1
                    except IntegrityError:
                        _connection.rollback()
    else:
        try:
            _connection.execute(insert, data)
            _connection.commit()
            _op_counter += 1
            _ins_counter += 1
        except IntegrityError:
            _connection.rollback()

    if _is_first_call:
        _connection.close()
    return _op_counter, _ins_counter
