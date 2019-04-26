# -*- coding: utf-8 -*-

"""
Extend the power of declarative base.
"""

from __future__ import print_function
import math
from copy import deepcopy
from collections import OrderedDict

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

try:
    from ..utils import ensure_list, grouper_list, ensure_session
    from ..crud.updating import update_all
except:  # pragma: no cover
    from sqlalchemy_mate.utils import ensure_list, grouper_list, ensure_session
    from sqlalchemy_mate.crud.updating import update_all

try:  # for type hint only
    from sqlalchemy import Table
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm.session import Session
    from sqlalchemy.sql.selectable import Select
    from sqlalchemy.sql.elements import TextClause
    from typing import Union, List
except ImportError:  # pragma: no cover
    pass


class ExtendedBase(object):
    """
    Provide additional method.

    Example::

        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        class User(Base, ExtendedBase):
            ... do what you do with sqlalchemy ORM
    """
    _cache_pk_names = None
    _cache_id_field_name = None
    _cache_keys = None

    _settings_major_attrs = None  # type: list
    _settings_engine = None  # type: Engine

    _long_live_session = None  # type: Session

    @classmethod
    def make_session(cls):
        if cls._settings_engine is None:
            raise TypeError
        if not isinstance(cls._settings_engine, Engine):
            raise TypeError
        try:
            cls.close_session().close()
        except:
            pass
        cls._long_live_session = sessionmaker(bind=cls._settings_engine)()

    @classmethod
    def close_session(cls):
        cls._long_live_session.close()
        cls._long_live_session = None

    @classmethod
    def get_eng(cls):
        if cls._settings_engine is None:
            raise NotImplementedError("you have to specify the engine at `_settings_engine` attribute!")
        return cls._settings_engine

    get_engine = get_eng

    @classmethod
    def get_ses(cls):
        if cls._long_live_session is None:
            cls.make_session()
        return cls._long_live_session

    get_session = get_ses

    @classmethod
    def _get_primary_key_names(cls):
        return tuple([col.name for col in inspect(cls).primary_key])

    @classmethod
    def pk_names(cls):
        """
        Primary key column name list.

        :rtype: tuple
        """
        if cls._cache_pk_names is None:
            cls._cache_pk_names = cls._get_primary_key_names()
        return cls._cache_pk_names

    def pk_values(self):
        """
        Primary key values
        """
        return tuple([getattr(self, name) for name in self.pk_names()])

    @classmethod
    def id_field_name(cls):
        """
        If only one primary_key, then return it. Otherwise, raise ValueError.

        :rtype: str
        """
        if cls._cache_id_field_name is None:
            pk_names = cls.pk_names()
            if len(pk_names) == 1:
                cls._cache_id_field_name = pk_names[0]
            else:  # pragma: no cover
                raise ValueError(
                    "{classname} has more than 1 primary key!"
                        .format(classname=cls.__name__)
                )
        return cls._cache_id_field_name

    def id_field_value(self):
        return getattr(self, self.id_field_name())

    @classmethod
    def keys(cls):
        """
        return list of all declared columns.

        :rtype: List[str]
        """
        if cls._cache_keys is None:
            cls._cache_keys = [c.name for c in cls.__table__._columns]
        return cls._cache_keys

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

        :rtype: dict
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

        :rtype: OrderedDict
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

    def glance(self):  # pragma: no cover
        """
        Print itself, only display attributes defined in
        :attr:`ExtendedBase._settings_major_attrs`
        """
        if self._settings_major_attrs is None:
            msg = ("Please specify attributes you want to include "
                   "in `class._settings_major_attrs`!")
            raise NotImplementedError(msg)

        kwargs = [
            (attr, getattr(self, attr))
            for attr in self._settings_major_attrs
        ]

        text = "{classname}({kwargs})".format(
            classname=self.__class__.__name__,
            kwargs=", ".join([
                "%s=%r" % (attr, value)
                for attr, value in kwargs
            ])
        )

        print(text)

    def absorb(self, other, ignore_none=True):
        """
        For attributes of others that value is not None, assign it to self.

        :type data: ExtendedBase
        :type ignore_none: bool

        **中文文档**

        将另一个文档中的数据更新到本条文档。当且仅当数据值不为None时。
        """
        if not isinstance(other, self.__class__):
            raise TypeError("`other` has to be a instance of %s!" %
                            self.__class__)

        if ignore_none:
            for attr, value in other.items():
                if value is not None:
                    setattr(self, attr, deepcopy(value))
        else:
            for attr, value in other.items():
                setattr(self, attr, deepcopy(value))

        return self

    def revise(self, data, ignore_none=True):
        """
        Revise attributes value with dictionary data.

        :type data: dict
        :type ignore_none: bool

        **中文文档**

        将一个字典中的数据更新到本条文档。当且仅当数据值不为None时。
        """
        if not isinstance(data, dict):
            raise TypeError("`data` has to be a dict!")

        if ignore_none:
            for key, value in data.items():
                if value is not None:
                    setattr(self, key, deepcopy(value))
        else:
            for key, value in data.items():
                setattr(self, key, deepcopy(value))

        return self

    @classmethod
    def by_id(cls, _id, engine_or_session):
        """
        Get one object by primary_key value.

        :type engine_or_session: Union[Engine, Session]
        """
        ses, auto_close = ensure_session(engine_or_session)
        obj = ses.query(cls).get(_id)
        if auto_close:
            ses.close()
        return obj

    @classmethod
    def by_sql(cls, sql, engine_or_session):
        """
        Query with sql statement or texture sql.

        :type sql: Union[Select, TextClause]
        :type engine_or_session: Union[Engine, Session]
        """
        ses, auto_close = ensure_session(engine_or_session)
        result = ses.query(cls).from_statement(sql).all()
        if auto_close:
            ses.close()
        return result

    @classmethod
    def smart_insert(cls, engine_or_session, data, minimal_size=5, op_counter=0):
        """
        An optimized Insert strategy.

        :type engine_or_session: Union[Engine, Session]
        :type data: Union[ExtendedBase, List[ExtendedBase]]
        :type minimal_size: int
        :type op_counter: int

        :return: number of insertion operation been executed. Usually it is
            greatly smaller than ``len(data)``.

        .. warning::

            This operation is not atomic, if you force stop the program,
            then it could be only partially completed

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
        ses, auto_close = ensure_session(engine_or_session)

        if isinstance(data, list):
            # 首先进行尝试bulk insert
            try:
                ses.add_all(data)
                ses.commit()
                op_counter += 1
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
                        op_counter = cls.smart_insert(
                            ses, chunk, minimal_size, op_counter)
                # 否则则一条条地逐条插入
                else:
                    for obj in data:
                        try:
                            ses.add(obj)
                            ses.commit()
                            op_counter += 1
                        except (IntegrityError, FlushError):
                            ses.rollback()
        else:
            try:
                ses.add(data)
                ses.commit()
            except (IntegrityError, FlushError):
                ses.rollback()

        if auto_close:
            ses.close()

        return op_counter

    @classmethod
    def update_all(cls, engine, obj_or_data, upsert=False):
        """
        The :meth:`sqlalchemy.crud.updating.update_all` function in ORM syntax.

        :type engine: Engine
        :param engine: an engine created by``sqlalchemy.create_engine``.

        :type obj_or_data: Union[ExtendedBase, List[ExtendedBase]]
        :param obj_or_data: single object or list of object

        :type upsert: bool
        :param upsert: if True, then do insert also.
        """
        obj_or_data = ensure_list(obj_or_data)
        update_all(
            engine=engine,
            table=cls.__table__,
            data=[obj.to_dict(include_null=False) for obj in obj_or_data],
            upsert=upsert,
        )

    @classmethod
    def upsert_all(cls, engine, obj_or_data):
        """
        The :meth:`sqlalchemy.crud.updating.upsert_all` function in ORM syntax.

        :type engine: Engine
        :param engine: an engine created by``sqlalchemy.create_engine``.

        :type obj_or_data: Union[ExtendedBase, List[ExtendedBase]]
        :param obj_or_data: single object or list of object
        """
        cls.update_all(
            engine=engine,
            obj_or_data=obj_or_data,
            upsert=True,
        )

    @classmethod
    def random(cls, engine_or_session, limit=5):
        """
        Return random ORM instance.

        :type engine_or_session: Union[Engine, Session]
        :type limit: int

        :rtype: List[ExtendedBase]
        """
        ses, auto_close = ensure_session(engine_or_session)
        result = ses.query(cls).order_by(func.random()).limit(limit).all()
        if auto_close:  # pragma: no cover
            ses.close()
        return result
