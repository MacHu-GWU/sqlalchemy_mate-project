.. _migrate-to-2x:

Migrate to sqlalchemy_mate 2.X
==============================================================================
From ``sqlalchemy_mate>=2.0.0.0``, it only support ``sqlalchemy>=2.0.0`` and only compatible with sqlalchemy 2.X API. Everything marked as ``no longer supported`` or ``no longer accepted`` in `SQLAlchemy 2.0 - Major Migration Guide <https://docs.sqlalchemy.org/en/20/changelog/migration_20.html#migration-core-connection-transaction>`_ document will no longer be supported from this version.

If your application has to run on ``sqlalchemy<2.0.0``, please use ``sqlalchemy_mate<=1.4.28.4``.
