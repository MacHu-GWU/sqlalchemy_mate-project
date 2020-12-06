.. _release_history:

Release and Version History
===========================


0.0.10 (TODO)
~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.0.11 (2020-12-05)
~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- ``ExtendedBase.by_id`` is renamed to ``ExtendedBase.by_pk``. The old method name is kept for backward API compatibility.
- add ``ExtendedBase.pk_fields`` method
- refact ``ExtendedBase.update_all`` method, allow working with session

**Minor Improvements**

- move CI to GitHub Action.
- add unit test on Windows

**Bugfixes**

**Miscellaneous**

- use in-package timeout_decorator library to ensure api compatibility


0.0.10 (2019-04-26)
~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

add type hint


0.0.9 (2019-04-26)
~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- pretty table ``from_everything`` now support textual sql
- add ``ExtendedBase.random()`` method

**Minor Improvements**

- More edge case test
- allow user to assign engine to ``ExtendedBase._settings_engine`` and then access engine and session with ``ExtendedBase.get_eng()``, ``ExtendedBase.get_ses()``

**Bugfixes**

**Miscellaneous**

- include type hint!


0.0.8 (2019-03-04)
~~~~~~~~~~~~~~~~~~
**Bugfixes**

- fix import error in ``Credential.from_env`` with AWS KMS.

**Miscellaneous**

- allow ``EngineCreator`` to return sqlalchemy connect string.
- improved docs


0.0.7 (2019-03-02)
~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add ``test_connection(engine, timeout=3)`` function.
- integrate ``Credential.from_env`` with AWS Key management Service.

**Miscellaneous**

- Deprecating ``sqlalchemy_mate.engine_creator``


0.0.6 (2019-03-02)
~~~~~~~~~~~~~~~~~~

**Bugfixes**

- add ``import boto3`` in ``Credential.from_s3_json()``


0.0.5 (2019-03-01)
~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- ``ExtendedBase.keys()`` now is a class method.
- ``ExtendedBase.glance()`` can print major attributes and values.
- **A New DB Credential reader** ``from sqlalchemy_mate import Credential, EngineCreator``

**Minor Improvements**

- change ``FromClause.count()`` -> ``func.count()``, since previous one will be deprecated soon in sqlalchemy.


0.0.4 (2018-08-11)
~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add ``ExtendedBase.pk_names``, ``ExtendedBase.id_field_name``, ``ExtendedBase.by_id``, ``ExtendedBase.by_sql``, ``ExtendedBase.update_all``, ``ExtendedBase.upsert_all``.

**Minor Improvements**

- use ``pygitrepo==0.0.21``

**Miscellaneous**

- Now ``ExtendedBase.smart_insert`` method returns number of insertion operation. So you can see the difference now.


0.0.3 (2018-07-22)
~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add a ``ExtendedBase`` class to give orm Declaritive Base more useful method.
- add a new method performs ``smart_insert`` in orm. It is 10 times faster in average than one by one insert. Can do bulk insert even there is a ``IntegrityError``.
- add a new ``engine_creator`` module to quickly create engines.

**Minor Improvements**

- now ``.crud.select, .crud.insert, .crud.update`` are renamed to ``.crud.selecting, .crud.inserting, .crud.updateing``.
- greately improved the doc strings.

**Bugfixes**

- fix a bug that returns different column name in export query result to ``PrettyTable``.

**Miscellaneous**

- improve testing coverage from 60% to 100%.
- add unittest for import.
- add documentation site.


0.0.2 (2018-07-03)
~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add more function can create PrettyTable from orm query, orm object, sql statement, table.

**Minor Improvements**

**Bugfixes**

- fix a bug that sometimes prettytable using bytes str for column name, now it ensures unicode str.

**Miscellaneous**


0.0.1 (2017-06-15)
~~~~~~~~~~~~~~~~~~
- First release
- Add ``insert``, ``select``, ``update``, ``io``, ``pt`` module.