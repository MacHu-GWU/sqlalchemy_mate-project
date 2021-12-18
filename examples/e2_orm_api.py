# -*- coding: utf-8 -*-

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, Session
import sqlalchemy_mate as sam

Base = declarative_base()

class User(Base, sam.ExtendedBase):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=True)

    _settings_major_attrs = [id, ]

#--- No DB interaction API
user = User(id=1, name="Alice")
print(user)
print(User.keys()) # class would work
print(user.keys())
print(user.values())
print(user.items())
print(user.to_dict())
print(user.to_OrderedDict())
print(user.pk_values()) # (1,)
user.glance()

user_bob = User(name="Bob")
user.absorb(user_bob)
print(user)

user.revise({"name": "Cathy"})
print(user)

print(User.pk_names())
print(User.pk_fields())

#--- DB interaction API
engine = sa.create_engine("sqlite:///:memory:")
sam.test_connection(engine, timeout=3)

Base.metadata.create_all(engine)

# Delete all data, make sure we have a fresh start
User.delete_all(engine)
# Count rows in a table
print(User.count_all(engine)) # 0

# Bulk insert
user_list = [
    User(id=57),
    User(id=264),
    User(id=697),
]
User.smart_insert(engine, user_list)
print(User.count_all(engine)) # 3

# Get single object by primary key values
user = User.by_pk(engine, 57)
print(user)

# Bulk insert, handle primary key conflicts efficiently
user_list = [
    User(id=id_)
    for id_ in range(1, 1000+1)
]
User.smart_insert(engine, user_list)
print(User.count_all(engine)) # 1000

# Bulk update + insert, locate rows by primary key values
user_list = [
    User(id=999, name="Alice"),
    User(id=1000, name="Bob"),
    User(id=1001, name="Cathy"),
    User(id=1002, name="David"),
]
User.upsert_all(engine, user_list)

print(User.by_pk(engine, 999).name) # Alice
print(User.by_pk(engine, 1001).name) # Cathy
print(User.count_all(engine)) # 1002

# Run raw SQL query
results = User.by_sql(
    engine,
    sql="""
    SELECT * 
    FROM users
    WHERE users.id >= 999
    """
)
print(results)
