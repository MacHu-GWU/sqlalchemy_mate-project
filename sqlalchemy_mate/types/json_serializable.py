# -*- coding: utf-8 -*-


try:
    from superjson import json
except ImportError:  # pragma: no cover
    import json
except:  # pragma: no cover
    import json
import sqlalchemy as sa


class JSONSerializableType(sa.types.TypeDecorator):
    """
    This column store json serialized python object in form of

    This column should be a json serializable python type such as combination of
    list, dict, string, int, float, bool.
    """
    impl = sa.UnicodeText
    cache_ok = True

    _FACTORY_CLASS = "factory_class"

    def __init__(self, *args, **kwargs):
        if self._FACTORY_CLASS not in kwargs:
            raise ValueError(
                (
                    "'JSONSerializableType' take only ONE argument {}, "
                    "it is the generic type that has ``to_json(self): -> str``, "
                    "and ``from_json(cls, value: str):`` class method."
                ).format(self._FACTORY_CLASS)
            )
        self.factory_class = kwargs.pop(self._FACTORY_CLASS)
        super(JSONSerializableType, self).__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        return self.impl

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            return value.to_json()

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return self.factory_class.from_json(value)
