.. _orm-api:

ORM API
==============================================================================


Extended Declarative Base
------------------------------------------------------------------------------
``sqlalchemy_mate.ExtendedBase`` is a Mixin class that enables many convenient method for your ORM model. In this

.. code-block:: python

    import sqlalchemy as sa
    import sqlalchemy.orm as orm
    import sqlalchemy_mate.api as sam

    Base = orm.declarative_base()

    # add sqlalchemy_mate.ExtendedBase Mixin class
    class User(Base, sam.ExtendedBase):
        __tablename__ = "users"

        id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
        name: orm.Mapped[str] = orm.mapped_column(sa.String, nullable=True)

        # put important columns here
        # you can choose to print those columns only with ``.glance()`` method.
        _settings_major_attrs = [id, ]

A data model should be have a nicer print:

.. code-block:: python

    user = User(id=1, name="Alice")
    print(user)
    # User(id=1, name='Alice')

Convert data model to / from generic python data type should be easy:

.. code-block:: python

    print(User.keys()) # class would work too, # ['id', 'name']
    print(user.keys()) # ['id', 'name']
    print(user.values()) # [1, 'Alice']
    print(user.items()) # [('id', 1), ('name', 'Alice')]
    print(user.to_dict()) # {'id': 1, 'name': 'Alice'}
    print(user.to_OrderedDict()) # OrderedDict([('id', 1), ('name', 'Alice')])
    print(user.pk_values()) # (1,)
    user.glance() # User(id=1)

Python dict can update values based on another dict, A data model should be able to do it to.

.. code-block:: python

    # update values based on another data model
    user_bob = User(name="Bob")
    user.absorb(user_bob)
    print(user) # User(id=1, name='Bob')

    # update values based on generic python dict
    user.revise({"name": "Cathy"})
    print(user) # User(id=1, name='Cathy')


Insert, Select, Update
------------------------------------------------------------------------------
Talk is cheap, show me the Code.

.. code-block:: python

    from sqlalchemy.orm import Session

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
    print(user) # User(id=57)

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
    # [User(id=999, name='Alice'), User(id=1000, name='Bob'), User(id=1001, name='Cathy'), User(id=1002, name='David')]
    print(results)
