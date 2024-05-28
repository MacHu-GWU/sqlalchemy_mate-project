# -*- coding: utf-8 -*-

import jsonpickle


# a custom python class
class ComputerDetails:
    def __init__(self, os: str, cpu: int, memory: int, disk: int):
        self.os = os
        self.cpu = cpu
        self.memory = memory
        self.disk = disk

    def to_json(self) -> str:
        return jsonpickle.encode(self)

    @classmethod
    def from_json(cls, json_str: str) -> "Computer":
        return jsonpickle.decode(json_str)


import sqlalchemy as sa
import sqlalchemy.orm as orm
import sqlalchemy_mate.api as sam

Base = orm.declarative_base()


class Computer(Base):
    __tablename__ = "computer"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer, primary_key=True)
    details: orm.Mapped[ComputerDetails] = orm.mapped_column(
        sam.types.JSONSerializableType(factory_class=ComputerDetails),
        nullable=True,
    )


engine = sam.engine_creator.EngineCreator().create_sqlite(
    "/tmp/sqlalchemy_mate_json_serializable.sqlite"
)
Base.metadata.create_all(engine)
sam.deleting.delete_all(engine, Computer.__table__)


with orm.Session(engine) as ses:
    computer = Computer(
        id=1,
        details=ComputerDetails(
            os="Linux",
            cpu=4,
            memory=8,
            disk=256,
        ),
    )

    ses.add(computer)
    ses.commit()

    computer = ses.get(Computer, 1)
    print(f"{computer.details.os = }")
    print(f"{computer.details.cpu = }")
    print(f"{computer.details.memory = }")
    print(f"{computer.details.disk = }")


t_computer = sa.Table(
    "computer",
    sa.MetaData(),
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("details", sa.String),
)


with engine.connect() as conn:
    stmt = sa.select(t_computer)
    for row in conn.execute(stmt).all():
        print(row)
