Release and Version History
===========================


0.0.4 (TODO)
~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


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