import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

# An Postgres DB example
# First, you use EngineCreator class to create the db connection specs
# Second, you choose to use which python driver, IDE will tell you
# all options you have
engine_psql = sam.EngineCreator(
    username="postgres",
    password="password",
    database="postgres",
    host="localhost",
    port=40311,
).create_postgresql_pg8000()

# You can use test_connection method to perform test connection and
# raise error if timeout.
sam.test_connection(engine_psql, timeout=3)

# A sqlite example
engine_sqlite = sam.EngineCreator().create_sqlite(path="/tmp/db.sqlite")
sam.test_connection(engine_sqlite, timeout=1)


Base = orm.declarative_base()


class User(Base, sam.ExtendedBase):
    __tablename__ = "users"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String, nullable=True)


t_users = User.__table__

engine = engine_sqlite
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
    ],
)

# pretty table from ORM class
print(sam.pt.from_everything(everything=User, engine_or_session=engine, limit=10))

# from Table
print(sam.pt.from_everything(t_users, engine, limit=3))

# from ORM styled select statement
print(
    sam.pt.from_everything(
        sa.select(User.name).where(User.id >= 4).limit(2),
        engine,
    )
)

# from SQL expression styled select statement
print(
    sam.pt.from_everything(
        sa.select(t_users.c.name).where(User.id >= 4),
        engine,
    )
)

# from Raw SQL text
print(
    sam.pt.from_everything(
        "SELECT id FROM users WHERE name = 'Edward'",
        engine,
    )
)

# from list of dict
print(
    sam.pt.from_everything(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Cathy"},
        ]
    )
)
