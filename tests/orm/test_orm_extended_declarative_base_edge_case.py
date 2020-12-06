# -*- coding: utf-8 -*-

import pytest
from sqlalchemy_mate.tests import (
    IS_WINDOWS,
    engine_sqlite, engine_psql, BaseClassForOrmTest,
    Base, BankAccount, PostTagAssociation,
)


class ExtendedBaseEdgeCaseTestBase(BaseClassForOrmTest):
    def test_absorb(self):
        user1 = BankAccount(id=1, name="Alice", pin="1234")
        user2 = BankAccount(name="Bob")
        user1.absorb(user2)
        assert user1.values() == [1, "Bob", "1234"]

        user1 = BankAccount(id=1, name="Alice", pin="1234")
        user2 = BankAccount(name="Bob")
        user1.absorb(user2, ignore_none=False)
        assert user1.values() == [None, "Bob", None]

    def test_revise(self):
        user1 = BankAccount(id=1, name="Alice", pin="1234")
        user1.revise(dict(name="Bob"))
        assert user1.values() == [1, "Bob", "1234"]

        user1 = BankAccount(id=1, name="Alice", pin="1234")
        user1.revise(dict(name="Bob", pin=None))
        assert user1.values() == [1, "Bob", "1234"]

        user1 = BankAccount(id=1, name="Alice", pin="1234")
        user1.revise(dict(name="Bob", pin=None), ignore_none=False)
        assert user1.values() == [1, "Bob", None]

    def test_by_pk(self):
        PostTagAssociation._settings_engine = self.engine

        PostTagAssociation.smart_insert(
            PostTagAssociation.get_eng(), [
                PostTagAssociation(post_id=1, tag_id=1, description="1-1"),
                PostTagAssociation(post_id=1, tag_id=2, description="1-2"),
            ]
        )
        PostTagAssociation.smart_insert(
            PostTagAssociation.get_ses(), [
                PostTagAssociation(post_id=1, tag_id=3, description="1-3"),
                PostTagAssociation(post_id=1, tag_id=4, description="1-4"),
            ]
        )

        pta = PostTagAssociation.by_pk((1, 2), self.ses)
        assert pta.post_id == 1
        assert pta.tag_id == 2

    def test_engine_and_session(self):
        PostTagAssociation._settings_engine = self.engine

        PostTagAssociation.make_session()
        PostTagAssociation.close_session()
        PostTagAssociation.get_eng()
        PostTagAssociation.get_ses()
        PostTagAssociation.close_session()


class TestExtendedBaseOnSqlite(ExtendedBaseEdgeCaseTestBase):  # test on sqlite
    engine = engine_sqlite
    declarative_base_class = Base


@pytest.mark.skipif(IS_WINDOWS)
class TestExtendedBaseOnPostgres(ExtendedBaseEdgeCaseTestBase):  # test on postgres
    engine = engine_psql
    declarative_base_class = Base


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
