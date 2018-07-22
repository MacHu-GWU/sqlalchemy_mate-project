#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extend the power of declarative base.
"""

import math
from copy import deepcopy
from collections import OrderedDict

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError

try:
    from ..utils import grouper_list
except:  # pragma: no cover
    from sqlalchemy_mate.utils import grouper_list


class ExtendedBase(object):
    """
    Provide additional method.

    Example::

        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        class User(Base, ExtendedBase):
            ... do what you do with sqlalchemy ORM
    """

    def keys(self):
        """
        return list of all declared columns.
        """
        return [c.name for c in self.__table__._columns]

    def values(self):
        """
        return list of value of all declared columns.
        """
        return [getattr(self, c.name, None) for c in self.__table__._columns]

    def items(self):
        """
        return list of pair of name and value of all declared columns.
        """
        return [
            (c.name, getattr(self, c.name, None))
            for c in self.__table__._columns
        ]

    def __repr__(self):
        kwargs = list()
        for attr, value in self.items():
            kwargs.append("%s=%r" % (attr, value))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(kwargs))

    def __str__(self):
        return self.__repr__()

    def to_dict(self, include_null=True):
        """
        Convert to dict.
        """
        if include_null:
            return dict(self.items())
        else:
            return {
                attr: value
                for attr, value in self.__dict__.items()
                if not attr.startswith("_sa_")
            }

    def to_OrderedDict(self, include_null=True):
        """
        Convert to OrderedDict.
        """
        if include_null:
            return OrderedDict(self.items())
        else:
            items = list()
            for c in self.__table__._columns:
                try:
                    items.append((c.name, self.__dict__[c.name]))
                except KeyError:
                    pass
            return OrderedDict(items)

    def absorb(self, other):
        """
        For attributes of others that value is not None, assign it to self.

        **中文文档**

        将另一个文档中的数据更新到本条文档。当且仅当数据值不为None时。
        """
        if not isinstance(other, self.__class__):
            raise TypeError("`other` has to be a instance of %s!" %
                            self.__class__)

        for attr, value in other.items():
            if value is not None:
                setattr(self, attr, deepcopy(value))

    def revise(self, data):
        """
        Revise attributes value with dictionary data.

        **中文文档**

        将一个字典中的数据更新到本条文档。当且仅当数据值不为None时。
        """
        if not isinstance(data, dict):
            raise TypeError("`data` has to be a dict!")

        for key, value in data.items():
            if value is not None:
                setattr(self, key, deepcopy(value))

    @classmethod
    def smart_insert(cls, engine_or_session, data, minimal_size=5):
        """An optimized Insert strategy.

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
        if isinstance(engine_or_session, Engine):
            ses = sessionmaker(bind=engine_or_session)()
            auto_close = True
        elif isinstance(engine_or_session, Session):
            ses = engine_or_session
            auto_close = False

        if isinstance(data, list):
            # 首先进行尝试bulk insert
            try:
                ses.add_all(data)
                ses.commit()
            # 失败了
            except (IntegrityError, FlushError):
                ses.rollback()
                # 分析数据量
                n = len(data)
                # 如果数据条数多于一定数量
                if n >= minimal_size ** 2:
                    # 则进行分包
                    n_chunk = math.floor(math.sqrt(n))
                    for chunk in grouper_list(data, n_chunk):
                        cls.smart_insert(ses, chunk, minimal_size)
                # 否则则一条条地逐条插入
                else:
                    for obj in data:
                        try:
                            ses.add(obj)
                            ses.commit()
                        except (IntegrityError, FlushError):
                            ses.rollback()
        else:
            try:
                ses.add(data)
                ses.commit()
            except (IntegrityError, FlushError):
                ses.rollback()

        ses.close()
