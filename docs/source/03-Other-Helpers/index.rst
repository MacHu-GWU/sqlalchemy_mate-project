.. _other}-helpers:

Other Helpers
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


User Friendly Engine Creator
------------------------------------------------------------------------------

`This sqlalchemy Official Document <https://docs.sqlalchemy.org/en/latest/core/engines.html>`_ tells you the correct connection string to use for different DB driver. Who wants to Google the API document everytime?

``sqlalchemy_mate.EngineCreator`` leveraged the IDE / Code Editor that provide a user friendly interface to pass in DB connection specs and choose the underlying python driver.

.. code-block:: python

    import sqlalchemy_mate as sam

    # An Postgres DB example
    # First, you use EngineCreator class to create the db connection specs
    # Second, you choose to use which python driver, IDE will tell you
    # all options you have
    engine_psql = sam.EngineCreator(
        username="postgres",
        password="password",
        database="postgres",
        host="localhost",
        port=43347,
    ).create_postgresql_psycopg2()

    # You can use test_connection method to perform test connection and
    # raise error if timeout.
    sam.test_connection(engine_sqlite, timeout=3)

    # A sqlite example
    engine_sqlite = sam.EngineCreator().create_sqlite(path="/tmp/db.sqlite")
    sam.test_connection(engine_sqlite, timeout=1)

For more information, see :mod:`~sqlalchemy_mate.engine_creator`


Ascii Table Printer
------------------------------------------------------------------------------

Lots of CLI DB client can print result in pretty Ascii Table. ``sqlalchemy_mate`` can do that too.

First let's insert some sample data:

.. code-block:: python

    import sqlalchemy as sa
    from sqlalchemy.orm import declarative_base
    import sqlalchemy_mate as sam

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

**Now let's create some query and print Ascii table**:

.. code-block:: python

    import sqlalchemy_mate as sam

    # from ORM class
    print(sam.pt.from_everything(User, engine))
    +----+--------+
    | id |  name  |
    +----+--------+
    | 1  | Alice  |
    | 2  |  Bob   |
    | 3  | Cathy  |
    | 4  | David  |
    | 5  | Edward |
    | 6  | Frank  |
    | 7  | George |
    +----+--------+

    # from Table
    print(sam.pt.from_everything(t_users, engine, limit=3))
    +----+-------+
    | id |  name |
    +----+-------+
    | 1  | Alice |
    | 2  |  Bob  |
    | 3  | Cathy |
    +----+-------+

    # from ORM styled select statement
    print(sam.pt.from_everything(
        sa.select(User.name).where(User.id >= 4).limit(2),
        engine,
    ))
    +--------+
    |  name  |
    +--------+
    | David  |
    | Edward |
    +--------+

    # from SQL expression styled select statement
    print(sam.pt.from_everything(
        sa.select(t_users.c.name).where(User.id >= 4),
        engine
    ))
    +--------+
    |  name  |
    +--------+
    | David  |
    | Edward |
    | Frank  |
    | George |
    +--------+

    # from Raw SQL text
    print(sam.pt.from_everything(
        "SELECT id FROM users WHERE name = 'Edward'",
        engine
    ))
    +----+
    | id |
    +----+
    | 5  |
    +----+

    # from list of dict
    print(sam.pt.from_everything([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Cathy"},
    ]))
    +----+-------+
    | id |  name |
    +----+-------+
    | 1  | Alice |
    | 2  |  Bob  |
    | 3  | Cathy |
    +----+-------+

For more information, see :mod:`~sqlalchemy_mate.pt`
