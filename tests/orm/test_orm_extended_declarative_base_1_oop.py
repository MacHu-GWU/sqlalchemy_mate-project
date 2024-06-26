# -*- coding: utf-8 -*-

"""
Object Oriented Programming related methods. No actual DB interaction happens.
"""

from collections import OrderedDict

from pytest import raises

from sqlalchemy_mate.tests.api import (
    User,
    Association,
)


class TestExtendedBaseOOP:
    def test_keys(self):
        assert User.keys() == ["id", "name"]
        assert User(id=1).keys() == ["id", "name"]

    def test_values(self):
        assert User(id=1, name="Alice").values() == [1, "Alice"]
        assert User(id=1).values() == [1, None]

    def test_items(self):
        assert User(id=1, name="Alice").items() == [("id", 1), ("name", "Alice")]
        assert User(id=1).items() == [("id", 1), ("name", None)]

    def test_repr(self):
        user = User(id=1, name="Alice")
        assert str(user) == "User(id=1, name='Alice')"

    def test_to_dict(self):
        assert User(id=1, name="Alice").to_dict() == {"id": 1, "name": "Alice"}
        assert User(id=1).to_dict() == {"id": 1, "name": None}
        assert User(id=1).to_dict(include_null=False) == {"id": 1}

    def test_to_OrderedDict(self):
        assert User(id=1, name="Alice").to_OrderedDict(
            include_null=True
        ) == OrderedDict(
            [
                ("id", 1),
                ("name", "Alice"),
            ]
        )

        assert User(id=1).to_OrderedDict(include_null=True) == OrderedDict(
            [
                ("id", 1),
                ("name", None),
            ]
        )
        assert User(id=1).to_OrderedDict(include_null=False) == OrderedDict(
            [
                ("id", 1),
            ]
        )

        assert User(name="Alice").to_OrderedDict(include_null=True) == OrderedDict(
            [
                ("id", None),
                ("name", "Alice"),
            ]
        )
        assert User(name="Alice").to_OrderedDict(include_null=False) == OrderedDict(
            [
                ("name", "Alice"),
            ]
        )

    def test_glance(self):
        user = User(id=1, name="Alice")
        user.glance(_verbose=False)

    def test_absorb(self):
        user1 = User(id=1)
        user2 = User(name="Alice")
        user3 = User(name="Bob")

        user1.absorb(user2)
        assert user1.to_dict() == {"id": 1, "name": "Alice"}
        user1.absorb(user3)
        assert user1.to_dict() == {"id": 1, "name": "Bob"}

        with raises(TypeError):
            user1.absorb({"name": "Alice"})

    def test_revise(self):
        user = User(id=1)
        user.revise({"name": "Alice"})
        assert user.to_dict() == {"id": 1, "name": "Alice"}

        user = User(id=1, name="Alice")
        user.revise({"name": "Bob"})
        assert user.to_dict() == {"id": 1, "name": "Bob"}

        with raises(TypeError):
            user.revise(User(name="Bob"))

    def test_primary_key_and_id_field(self):
        assert User.pk_names() == ("id",)
        assert tuple([field.name for field in User.pk_fields()]) == ("id",)
        assert User(id=1).pk_values() == (1,)

        assert User.id_field_name() == "id"
        assert User.id_field().name == "id"
        assert User(id=1).id_field_value() == 1

        assert Association.pk_names() == ("x_id", "y_id")
        assert tuple([field.name for field in Association.pk_fields()]) == (
            "x_id",
            "y_id",
        )
        assert Association(x_id=1, y_id=2).pk_values() == (1, 2)

        with raises(ValueError):
            Association.id_field_name()

        with raises(ValueError):
            Association.id_field()

        with raises(ValueError):
            Association(x_id=1, y_id=2).id_field_value()


if __name__ == "__main__":
    from sqlalchemy_mate.tests import run_cov_test

    run_cov_test(
        __file__, "sqlalchemy_mate.orm.extended_declarative_base", preview=False
    )
