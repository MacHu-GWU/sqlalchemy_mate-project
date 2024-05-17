.. _core-api:

Core API
==============================================================================
In this section, we demonstrate the simplified version with ``sqlalchemy_mate`` to manipulate data using core API.

First, let's define a table to get start, everything looks normal so far.

.. code-block:: python

    import sqlalchemy as sa

    metadata = sa.MetaData()

    t_users = sa.Table(
        "users", metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String, nullable=True)
    )

    # For syntax testing, you could use sqlite
    # But you could see significant performance improvement in main stream
    # sql database for bulk inserts
    engine = sa.create_engine("sqlite:///:memory:")

    metadata.create_all(engine)


Bulk insert and Count rows
------------------------------------------------------------------------------

We want to insert 3 random user data into the database and do some basic query:

.. code-block:: python

    import random

    user_data_list = [
        dict(id=random.randint(1, 1000),
        for _ in range(3)
    ]

**With** ``sqlalchemy_mate``

.. code-block:: python

    import sqlalchemy_mate.api as sam

    # do bulk insert
    sam.inserting.smart_insert(engine, t_users, user_data_list)

    # returns number of row in a table.
    # should returns 3
    row_counts = sam.selecting.count_row(engine, t_users)

    # return one row by primary key values
    # {"id": 1, "name": None}
    user = sam.selecting.by_pk(engine, t_users, 1)

**Without** ``sqlalchemy_mate``

.. code-block:: python

    with engine.connect() as connection:
        # do bulk insert
        connection.execute(t_users.insert(), user_data_list)

        # returns number of row in a table.
        # should returns 3
        row_counts = connection.execute(sa.select(sa.func.count()).select_from(t_users)).one()[0]

       # return one row by primary key values
        # {"id": 1, "name": None}
        user = connection.execute(sa.select(t_users).where(t_users.c.id==1)).one()

Now the syntax sugar looks like just so so, let's move to the next example.


Smart Single / Bulk Insert
------------------------------------------------------------------------------

Now we already have 3 items in database, let's try to insert 1,000 users to the table.

.. code-block:: python

    user_data_list = [
        dict(id=id_)
        for id_ in range(1, 1000+1)
    ]

**With** ``sqlalchemy_mate``

.. code-block:: python

    # Core insert logic = 1 line
    import time

    start_time = time.process_time()
    op_count, ins_count = sam.inserting.smart_insert(engine, t_users, user_data_list)
    elapsed = time.process_time() - start_time
    print(op_count) # 60 bulk INSERT sql command fired to database
    print(ins_count) # 997 data inserted
    print(elapsed) # 0.019572 in local postgres database
    row_counts = sam.selecting.count_row(engine, t_users)
    print(row_counts) # now we have 1000 rows

**Without** ``sqlalchemy_mate``

.. code-block:: python

    # Core insert logic = 7 line
    import time
    from sqlalchemy.exc import IntegrityError

    start_time = time.process_time()
    with engine.connect() as connection:
        ins = t_users.insert()
        for user_data in user_data_list:
            try:
                connection.execute(ins, user_data)
            except IntegrityError:
                pass
        elapsed = time.process_time() - start_time
        print(elapsed) # 0.181163
        row_counts = connection.execute(sa.select(sa.func.count()).select_from(t_users)).one()[0]
        print(row_counts)

``sqlachemy_mate`` is significantly faster than native ``sqlalchemy``. Because it smartly split big dataset into smaller pack, hence the total number of ``INSERT sql`` actually fired to database is greatly reduced. In this test case, ``sqlclhemy_mate`` is 10x faster with a Postgres DB on local, in real use case it could save more times because they are remote user.


Smart Single / Bulk Update
------------------------------------------------------------------------------

A common update use case is to locate a row by primary key, and update non primary key fields.

**With** ``sqlalchemy_mate``

.. code-block:: python

    # update
    # before, it is {"id": 1, "name": None}
    print(sam.selecting.by_pk(engine, t_users, 1))

    # do single update
    user_data = dict(id=1, name="Alice")
    sam.updating.update_all(engine, t_users, user_data)

    # after, it is {"id": 1, "name": None}
    print(sam.selecting.by_pk(engine, t_users, 1))

    # do multiple update
    user_data_list = [
        dict(id=1, name="Alice"),
        dict(id=2, name="Bob"),
        dict(id=3, name="Cathy"),
    ]
    sam.updating.update_all(engine, t_users, user_data_list)

**Without** ``sqlalchemy_mate``

.. code-block:: python

    # do single update
    with engine.connect() as connection:
        connection.execute(t_users.update().where(t_users.c.id==1).values(name="Alice"))

    # do multiple update
    user_data_list = [
        dict(id=1, name="Alice"),
        dict(id=2, name="Bob"),
        dict(id=3, name="Cathy"),
    ]
    with engine.connect() as connection:
        for user in user_data_list:
            connection.execute(t_users.update().where(t_users.c.id==user["id"]).values(**user)


Smart Single Bulk Upsert
------------------------------------------------------------------------------

Upsert is a database dependent feature that not available in all sql system. :meth:`~sqlalchemy_mate.crud.updating.upsert_all`` function made upsert generally available to all SQL system and super easy to use. Internally there's an optimization that collect "to insert" items and bulk insert them fast.

**With** ``sqlalchemy_mate``

.. code-block:: python

    # prepare your data
    user_data_list = [
        dict(id=999, name="Alice"),
        dict(id=1000, name="Bob"),
        dict(id=1001, name="Cathy"),
        dict(id=1002, name="David"),
    ]

    # use ``upsert_all`` method
    sam.updating.upsert_all(engine, t_users, user_data_list)


Selecting Shortcuts
------------------------------------------------------------------------------

- See :mod:`~sqlalchemy_mate.crud.selecting`


Deleteing Short cuts
------------------------------------------------------------------------------

- See :mod:`~sqlalchemy_mate.crud.selecting`
