# -*- coding: utf-8 -*-

import sqlalchemy as sa
import sqlalchemy_mate as sam

metadata = sa.MetaData()

t_users = sa.Table(
    "users", metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String, nullable=True)
)

engine = sa.create_engine("sqlite:///:memory:")
sam.test_connection(engine, timeout=3)

metadata.create_all(engine)

user_data_list = [
    dict(id=57),
    dict(id=264),
    dict(id=697),
]
sam.inserting.smart_insert(engine, t_users, user_data_list)
row_counts = sam.selecting.count_row(engine, t_users)
print(f"there are {row_counts} rows in Table('{t_users.name}')")

user_data_list = [
    dict(id=id_)
    for id_ in range(1, 1000 + 1)
]

op_count, ins_count = sam.inserting.smart_insert(engine, t_users, user_data_list)
row_counts = sam.selecting.count_row(engine, t_users)
print(f"there are {row_counts} rows in Table('{t_users.name}')")

assert sam.selecting.by_pk(engine, t_users, 0) is None
user = sam.selecting.by_pk(engine, t_users, 1)
print(user._asdict())  # {'id': 1, 'name': None}

user_data = dict(id=1, name="Alice")
sam.updating.update_all(engine, t_users, user_data)
user = sam.selecting.by_pk(engine, t_users, 1)
print(user._asdict())  # {'id': 1, 'name': 'Alice'}
