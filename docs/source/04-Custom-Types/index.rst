.. _custom-types:

Custom Types
==============================================================================


Compressed String
------------------------------------------------------------------------------
A unicode string, but compressed.


Compressed Binary
------------------------------------------------------------------------------
A big binary blob, but compressed.


Compressed JSON
------------------------------------------------------------------------------
A json serializable object, but compressed.


JSON Serializable
------------------------------------------------------------------------------
Any JSON serializable object, if implemented ``to_json(self):`` and ``from_json(cls, json_str):`` method.

.. code-block:: python

    import sqlalchemy.orm as orm
    import jsonpickle

    # a custom python class
    class ComputerDetails:
        def __init__(self, ...):
            ...

        def to_json(self) -> str:
            return jsonpickle.encode(self)

        @classmethod
        def from_json(cls, json_str: str) -> 'Computer':
            return cls(**jsonpickle.decode(json_str))

    Base = orm.declarative_base()

    class Computer(Base):
        id = Column(Integer, primary_key)
        details = Column(JSONSerializableType(factory_class=Computer)

        ...

    computer = Computer(
        id=1,
        details=ComputerDetails(...),
    )

    with Session(engine) as session:
        session.add(computer)
        session.commit()

        computer = session.get(Computer, 1)
        print(computer.details)
