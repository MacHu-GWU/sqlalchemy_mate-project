# -*- coding: utf-8 -*-

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
import sqlalchemy_mate.api as sam

Base = declarative_base()


class User(Base, sam.ExtendedBase):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=True)


t_users = User.__table__

engine = sam.EngineCreator().create_sqlite()
Base.metadata.create_all(engine)
User.smart_insert(
    engine,
    [
        User(id=1, name="Alice"),
        User(id=2, name="Bob"),
        User(id=3, name="Cathy"),
        User(id=4, name="David"),
        User(id=5, name="Edward"),
        User(id=6, name="Frank"),
        User(id=7, name="George"),
    ]
)

# from ORM class
print(sam.pt.from_everything(User, engine))

# from Table
print(sam.pt.from_everything(t_users, engine, limit=3))

# from ORM styled select statement
print(sam.pt.from_everything(
    sa.select(User.name).where(User.id >= 4).limit(2),
    engine,
))

# from SQL expression styled select statement
print(sam.pt.from_everything(
    sa.select(t_users.c.name).where(User.id >= 4),
    engine
))

# from Raw SQL text
print(sam.pt.from_everything(
    "SELECT id FROM users WHERE name = 'Edward'",
    engine
))

# from list of dict
print(sam.pt.from_everything([
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Cathy"},
]))
