# -*- coding: utf-8 -*-

import json
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy_mate.types.json_serializable import JSONSerializableType
from sqlalchemy_mate.tests import IS_WINDOWS, engine_sqlite, engine_psql

Base = declarative_base()


class Profile:
    def __init__(self, dob: str):
        self.dob = dob

    def to_json(self) -> str:
        return json.dumps(dict(dob=self.dob))

    @classmethod
    def from_json(cls, value) -> 'Profile':
        return cls(**json.loads(value))


class User(Base):
    __tablename__ = "types_json_serializable_users"

    id: int = sa.Column(sa.Integer, primary_key=True)
    profile: Profile = sa.Column(JSONSerializableType(factory_class=Profile))


class JSONSerializableBaseTest:
    engine: Engine = None

    id_ = 1
    profile = Profile(dob="2021-01-01")

    @classmethod
    def setup_class(cls):
        if cls.engine is None:
            return None
        Base.metadata.drop_all(cls.engine)
        Base.metadata.create_all(cls.engine)

        with cls.engine.connect() as conn:
            conn.execute(User.__table__.delete())

        with Session(cls.engine) as ses:
            user = User(id=cls.id_, profile=cls.profile)
            ses.add(user)
            ses.commit()

    def test_exception(self):
        with pytest.raises(ValueError):
            JSONSerializableType()

    def test_read_and_write(self):
        with Session(self.engine) as ses:
            user = ses.get(User, self.id_)
            assert isinstance(user.profile, Profile)
            assert user.profile.dob == self.profile.dob

            user = User(id=2)
            ses.add(user)
            ses.commit()

            user = ses.get(User, 2)
            assert user.profile == None

    def test_select_where(self):
        with Session(self.engine) as ses:
            user = ses.scalars(sa.select(User).where(User.profile == self.profile)).one()
            assert user.profile.dob == self.profile.dob


class TestSqlite(JSONSerializableBaseTest):
    engine = engine_sqlite


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="no psql service container for windows",
)
class TestPsql(JSONSerializableBaseTest):  # pragma: no cover
    engine = engine_psql


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
