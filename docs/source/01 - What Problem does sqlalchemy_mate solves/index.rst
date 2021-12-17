Whats Problem does ``sqlalchemy_mate`` Solves?
==============================================
**Example1**, make your code succinct:

with ``sqlalchemy``::

    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Cathy"},
    ]

    # if database already have {"id": 2, "name": "Bob"}
    insert = table_user.insert()
    with engine.begin() as connection:
        for user_data in data:
            try:
                connection.execute(insert, user_data)
                connection.commit()
            except:
                connection.rollback()


with ``sqlalchemy_mate``::

    from sqlalchemy_mate import inserting

    data = [
        {"id": 1, "name": "Alice"}
        {"id": 2, "name": "Bob"}
        {"id": 3, "name": "Cathy"}
    ]
    inserting.smart_insert(engine, table_user, data)


And if there is conflict with the primary key, the second one is usually x10 faster. (There's another method works with ORM, see :meth:`~sqlalchemy_mate.orm.extended_declarative_base.ExtendedBase.smart_insert`)


**Example2**, free you from memorizing things, IDLE will tell you::

    from sqlalchemy_mate import engine_creator

    engine = engine_creator.create_sqlite() # in memory db
    engine = engine_creator.create_sqlite(path="test.sqlite")
    engine = engine_creator.create_mysql_mysqldb(
        username, password. host, port, database)


**Example3**, fun utility function::

    # print result in ascii table
    >>> from sqlalchemy_mate import pt
    >>> pt.from_everything(table_user, engine, limit=3)
    +---------+-------+
    | user_id |  name |
    +---------+-------+
    |    1    | Alice |
    |    2    |  Bob  |
    |    3    | Cathy |
    +---------+-------+


For more tools for select, insert, update, prettytable, io, please take a look at:

- :mod:`~sqlalchemy_mate.crud.selecting`
- :mod:`~sqlalchemy_mate.crud.inserting`
- :mod:`~sqlalchemy_mate.crud.updating`
- :mod:`~sqlalchemy_mate.io`
- :mod:`~sqlalchemy_mate.pt`
- :mod:`~sqlalchemy_mate.engine_creator`
