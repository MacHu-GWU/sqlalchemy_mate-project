# -*- coding: utf-8 -*-

import pytest
from sqlalchemy_mate.tests.api import (
    IS_WINDOWS,
    engine_sqlite,
    engine_psql,
    BankAccount,
    PostTagAssociation,
    BaseCrudTest,
)


class ExtendedBaseEdgeCaseTestBase(BaseCrudTest):
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
        PostTagAssociation.smart_insert(
            self.eng,
            [
                PostTagAssociation(post_id=1, tag_id=1, description="1-1"),
                PostTagAssociation(post_id=1, tag_id=2, description="1-2"),
            ],
        )
        PostTagAssociation.smart_insert(
            self.eng,
            [
                PostTagAssociation(post_id=1, tag_id=3, description="1-3"),
                PostTagAssociation(post_id=1, tag_id=4, description="1-4"),
            ],
        )

        pta = PostTagAssociation.by_pk(self.eng, (1, 2))
        assert pta.post_id == 1
        assert pta.tag_id == 2


class TestExtendedBaseOnSqlite(ExtendedBaseEdgeCaseTestBase):  # test on sqlite
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestExtendedBaseOnPostgres(ExtendedBaseEdgeCaseTestBase):  # test on postgres
    engine = engine_psql


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(
        __file__,
        "sqlalchemy_mate.orm.extended_declarative_base",
        preview=False,
    )
