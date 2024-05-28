# -*- coding: utf-8 -*-

import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam
from rich import print as rprint

Base = orm.declarative_base()


# add sqlalchemy_mate.ExtendedBase Mixin class
class User(Base, sam.ExtendedBase):
    __tablename__ = "users"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String, nullable=True)
    # you can also do this
    # id: int = sa.Column(sa.Integer, primary_key=True)
    # name: str = sa.Column(sa.String, nullable=True)

    # put important columns here
    # you can choose to print those columns only with ``.glance()`` method.
    _settings_major_attrs = [
        id,
    ]


user = User(id=1, name="Alice")
rprint(user)

rprint(User.keys())
rprint(user.keys())
rprint(user.values())
rprint(user.items())
rprint(user.to_dict())
rprint(user.to_OrderedDict())
rprint(user.pk_values())
user.glance()  # only print important columns

# update values based on another data model, it is similar to ``dict.update()``
user_bob = User(name="Bob")
user.absorb(user_bob)
rprint(user)

# update values based on generic python dict, it is similar to ``dict.update()``
user.revise({"name": "Cathy"})
rprint(user)

from sqlalchemy.orm import Session

engine = sa.create_engine("sqlite:///:memory:")
sam.test_connection(engine, timeout=3)

Base.metadata.create_all(engine)

# Delete all data, make sure we have a fresh start
User.delete_all(engine)
# Count rows in a table
print(User.count_all(engine))

# Bulk insert
user_list = [
    User(id=57),
    User(id=264),
    User(id=697),
]
User.smart_insert(engine, user_list)
rprint(User.count_all(engine))

# Get single object by primary key values
user = User.by_pk(engine, 57)
rprint(user)  # User(id=57)

# Bulk insert, handle primary key conflicts efficiently
user_list = [User(id=id_) for id_ in range(1, 1000 + 1)]
User.smart_insert(engine, user_list)
rprint(User.count_all(engine))  # 1000

# Bulk update + insert, locate rows by primary key values
user_list = [
    User(id=999, name="Alice"),
    User(id=1000, name="Bob"),
    User(id=1001, name="Cathy"),
    User(id=1002, name="David"),
]
User.upsert_all(engine, user_list)

rprint(User.by_pk(engine, 999).name)  # Alice
rprint(User.by_pk(engine, 1001).name)  # Cathy
rprint(User.count_all(engine))  # 1002

# Run raw SQL query
results = User.by_sql(
    engine,
    sql="""
    SELECT *
    FROM users
    WHERE users.id >= 999
    """,
)
rprint(results)
