# -*- coding: utf-8 -*-

from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy_mate import ExtendedBase

engine = sa.create_engine("sqlite:///:memory:", echo=False)
Session = sessionmaker(engine)


@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


Base = declarative_base()


class User(Base, ExtendedBase):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    username = sa.Column(sa.String)


class UserNew(Base, ExtendedBase):
    __tablename__ = "users_new"

    id = sa.Column(sa.Integer, primary_key=True)
    username = sa.Column(sa.String)
    email = sa.Column(sa.String)


Base.metadata.create_all(engine)

with session_scope() as session:
    session.add(User(id=1, username="Alice"))
    print(session.query(User).all())
    print(session.query(UserNew).all())

    ins = UserNew.__table__.insert().from_select(
        [
            UserNew.id,
            UserNew.username,
        ],
        sa.select([User.__table__])
    )
    engine.execute(ins)
    print(session.query(UserNew).all())

    User.__table__.drop(engine)
    engine.execute("ALTER TABLE users_new RENAME TO users;")

    Base1 = declarative_base()

    class User(Base1, ExtendedBase):
        __tablename__ = "users"

        id = sa.Column(sa.Integer, primary_key=True)
        username = sa.Column(sa.String)
        email = sa.Column(sa.String)

    print(session.query(User).all())
