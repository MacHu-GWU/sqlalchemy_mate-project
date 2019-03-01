.. image:: https://readthedocs.org/projects/sqlalchemy_mate/badge/?version=latest
    :target: https://sqlalchemy_mate.readthedocs.io/index.html
    :alt: Documentation Status

.. image:: https://travis-ci.org/MacHu-GWU/sqlalchemy_mate-project.svg?branch=master
    :target: https://travis-ci.org/MacHu-GWU/sqlalchemy_mate-project?branch=master

.. image:: https://codecov.io/gh/MacHu-GWU/sqlalchemy_mate-project/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/MacHu-GWU/sqlalchemy_mate-project

.. image:: https://img.shields.io/pypi/v/sqlalchemy_mate.svg
    :target: https://pypi.python.org/pypi/sqlalchemy_mate

.. image:: https://img.shields.io/pypi/l/sqlalchemy_mate.svg
    :target: https://pypi.python.org/pypi/sqlalchemy_mate

.. image:: https://img.shields.io/pypi/pyversions/sqlalchemy_mate.svg
    :target: https://pypi.python.org/pypi/sqlalchemy_mate

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/sqlalchemy_mate-project

------


.. image:: https://img.shields.io/badge/Link-Document-blue.svg
      :target: https://sqlalchemy_mate.readthedocs.io/index.html

.. image:: https://img.shields.io/badge/Link-API-blue.svg
      :target: https://sqlalchemy_mate.readthedocs.io/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Source_Code-blue.svg
      :target: https://sqlalchemy_mate.readthedocs.io/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
      :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
      :target: https://github.com/MacHu-GWU/sqlalchemy_mate-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
      :target: https://github.com/MacHu-GWU/sqlalchemy_mate-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
      :target: https://github.com/MacHu-GWU/sqlalchemy_mate-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
      :target: https://pypi.org/pypi/sqlalchemy_mate#files


Welcome to ``sqlalchemy_mate`` Documentation
==============================================================================

A library extend sqlalchemy module, makes CRUD easier.


Features
------------------------------------------------------------------------------


Read Database Credential Safely
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``sqlalchemy_mate`` allows

**From json file**::

content of json::

    {
        "credentials": {
            "db1": {
                "host": "example.com",
                "port": 1234,
                "database": "test",
                "username": "admin",
                "password": "admin",
            },
            "db2": {
                ...
            }
        }
    }

code::

    from sqlalchemy_mate import EngineCreator

    ec = EngineCreator.from_json(
        json_file="path-to-json-file",
        json_path="credentials.db1", # dot notation json path
    )
    engine = ec.create_postgresql_psycopg2()

Any json scheme should work.

**From** ``$HOME/.db.json``::

    from sqlalchemy_mate import EngineCreator

    ec = EngineCreator.from_home_db_json(identifier="db1")
    engine = ec.create_postgresql_psycopg2()

``$HOME/.db.json`` **assumes flat json schema**::

    {
        "identifier1": {
            "host": "example.com",
            "port": 1234,
            "database": "test",
            "username": "admin",
            "password": "admin",
        },
        "identifier2": {
            ...
        }
    }

**From json file on AWS S3**::

    from sqlalchemy_mate import EngineCreator

    ec = EngineCreator.from_s3_json(
        bucket_name="my-bucket", key="db.json",
        json_path="identifier1",
        aws_profile="my-profile",
    )
    engine = ec.create_redshift()

**From Environment Variable**::

    from sqlalchemy_mate import EngineCreator

    ec = EngineCreator.from_env(prefix="DB_DEV")
    engine = ec.create_redshift()


.. _install:

Install
------------------------------------------------------------------------------

``sqlalchemy_mate`` is released on PyPI, so all you need is:

.. code-block:: console

    $ pip install sqlalchemy_mate

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade sqlalchemy_mate