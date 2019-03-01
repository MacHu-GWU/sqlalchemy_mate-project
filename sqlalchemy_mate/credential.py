# -*- coding: utf-8 -*-

"""
Safe database credential loader.
"""

import os
import json
import string
import sqlalchemy as sa


class Credential(object):
    """
    Database credential.
    """

    def __init__(self, host=None, port=None, database=None,
                 username=None, password=None):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    uri_template = "{username}{has_password}{password}@{host}{has_port}{port}/{database}"
    path_db_json = os.path.join(os.path.expanduser("~"), ".db.json")
    local_home = os.path.basename(os.path.expanduser("~"))

    def __repr__(self):
        return "{classname}(host='{host}', port={port}, database='{database}', username={username}, password='xxxxxxxxxxxx')".format(
            classname=self.__class__.__name__,
            host=self.host, port=self.port,
            database=self.database, username=self.username,
        )

    @property
    def uri(self):
        """
        Return sqlalchemy connect string URI.
        """
        return self.uri_template.format(
            host=self.host,
            port="" if self.port is None else self.port,
            database=self.database,
            username=self.username,
            password="" if self.password is None else self.password,
            has_password="" if self.password is None else ":",
            has_port="" if self.port is None else ":",
        )

    @classmethod
    def _validate_key_mapping(cls, key_mapping):
        if key_mapping is not None:
            keys = list(key_mapping)
            keys.sort()
            if keys != ["database", "host", "password", "port", "username"]:
                msg = ("`key_mapping` is the credential field mapping from `Credential` to custom json! "
                       "it has to be a dictionary with 5 keys: "
                       "host, port, password, port, username!")
                raise ValueError(msg)

    @classmethod
    def _transform(cls, data, key_mapping):
        if key_mapping is None:
            return data
        else:
            return {actual: data[custom] for actual, custom in key_mapping.items()}

    @classmethod
    def _from_json_data(cls, data, json_path=None, key_mapping=None):
        if json_path is not None:
            for p in json_path.split("."):
                data = data[p]
        return cls(**cls._transform(data, key_mapping))

    @classmethod
    def from_json(cls, json_file, json_path=None, key_mapping=None):
        """
        Load connection credential from json file.

        :param json_file:
        :param json_path: dot notation of the path to the credential dict.
        :param key_mapping: map 'host', 'port', 'database', 'username', 'password'
            to custom alias, for example ``{'host': 'h', 'port': 'p', 'database': 'db', 'username': 'user', 'password': 'pwd'}``. This params are used to adapt any json data.
        :return:

        Example:

        Your json file::

            {
                "credentials": {
                    "db1": {
                        "h": "example.com",
                        "p": 1234,
                        "db": "test",
                        "user": "admin",
                        "pwd": "admin",
                    },
                    "db2": {
                        ...
                    }
                }
            }

        Usage::

            cred = Credential.from_json(
                "path-to-json-file", "credentials.db1",
                dict(host="h", port="p", database="db", username="user", password="pwd")
            )
        """
        cls._validate_key_mapping(key_mapping)
        with open(json_file, "rb") as f:
            data = json.loads(f.read().decode("utf-8"))
            return cls._from_json_data(data, json_path, key_mapping)

    @classmethod
    def from_home_db_json(cls, identifier, key_mapping=None):  # pragma: no cover
        """
        Read credential from $HOME/.db.json file.

        :param identifier: database identifier.

        ``.db.json````::

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
        """
        return cls.from_json(
            json_file=cls.path_db_json, json_path=identifier, key_mapping=key_mapping)

    @classmethod
    def from_s3_json(cls, bucket_name, key,
                     json_path=None, key_mapping=None,
                     aws_profile=None,
                     aws_access_key_id=None,
                     aws_secret_access_key=None,
                     region_name=None):  # pragma: no cover
        """
        Load database credential from json on s3.

        :param bucket_name:
        :param key:
        :param aws_profile: if None, assume that you are using this from
            AWS cloud. (service on the same cloud doesn't need profile name)
        :param local_home: if you set this to your $HOME username, then
            activate use of ``aws_profile`` to connect.
        """
        import boto3

        ses = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            profile_name=aws_profile,
        )
        s3 = ses.resource("s3")

        bucket = s3.Bucket(bucket_name)
        object = bucket.Object(key)
        data = json.loads(object.get()["Body"].read().decode("utf-8"))
        return cls._from_json_data(data, json_path, key_mapping)

    @classmethod
    def from_env(cls, prefix):  # pragma: no cover
        """
        Load database credential from env variable.

        :param prefix: PREFIX_HOST, PREFIX_PORT, ...
        """
        if len(prefix) < 1:
            raise ValueError("prefix can't be empty")
        if prefix[-1] in "!@#$%^&*()-=+|{}[]:;<>,.?/":
            raise ValueError("prefix can't end with '%s'" % prefix[-1])
        if len(set(string.ascii_lowercase).intersection(set(prefix))):
            raise ValueError("prefix has to be all uppercase!")
        if not prefix.endswith("_"):
            prefix = prefix + "_"
        return cls(
            host=os.getenv(prefix + "HOST"),
            port=int(os.getenv(prefix + "PORT")),
            database=os.getenv(prefix + "DATABASE"),
            username=os.getenv(prefix + "USERNAME"),
            password=os.getenv(prefix + "PASSWORD"),
        )


class EngineCreator(Credential):  # pragma: no cover
    def _create_engine(self, dialect_and_driver, **kwargs):
        conn_str = "{}://{}".format(dialect_and_driver, self.uri)
        return sa.create_engine(conn_str, **kwargs)

    _ce = _create_engine

    def create_sqlite(self, path=":memory:", **kwargs):
        """"""
        return sa.create_engine("sqlite:///{path}".format(path=path), **kwargs)

    # postgresql
    def create_postgresql(self, **kwargs):
        """"""
        return self._ce("postgresql", **kwargs)

    def create_postgresql_psycopg2(self, **kwargs):
        """"""
        return self._ce("postgresql+psycopg2", **kwargs)

    def create_postgresql_pg8000(self, **kwargs):
        """"""
        return self._ce("postgresql+pg8000", **kwargs)

    def _create_postgresql_pygresql(self, **kwargs):
        """"""
        return self._ce("postgresql+pygresql", **kwargs)

    def create_postgresql_psycopg2cffi(self, **kwargs):
        """"""
        return self._ce("postgresql+psycopg2cffi", **kwargs)

    def create_postgresql_pypostgresql(self, **kwargs):
        """"""
        return self._ce("postgresql+pypostgresql", **kwargs)

    # mysql
    def create_mysql(self, **kwargs):
        """"""
        return self._ce("mysql", **kwargs)

    def create_mysql_mysqldb(self, **kwargs):
        """"""
        return self._ce("mysql+mysqldb", **kwargs)

    def create_mysql_mysqlconnector(self, **kwargs):
        return self._ce("mysql+mysqlconnector", **kwargs)

    def create_mysql_oursql(self, **kwargs):
        """"""
        return self._ce("mysql+oursql", **kwargs)

    def create_mysql_pymysql(self, **kwargs):
        """"""
        return self._ce("mysql+pymysql", **kwargs)

    def create_mysql_cymysql(self, **kwargs):
        """"""
        return self._ce("mysql+cymysql", **kwargs)

    # oracle
    def create_oracle(self, **kwargs):
        """"""
        return self._ce("oracle", **kwargs)

    def create_oracle_cx_oracle(self, **kwargs):
        """"""
        return self._ce("oracle+cx_oracle", **kwargs)

    # mssql
    def create_mssql_pyodbc(self, **kwargs):
        """"""
        return self._ce("mssql+pyodbc", **kwargs)

    def create_mssql_pymssql(self, **kwargs):
        """"""
        return self._ce("mssql+pymssql", **kwargs)

    # redshift
    def create_redshift(self, **kwargs):
        """"""
        return self._ce("redshift+psycopg2", **kwargs)


if __name__ == "__main__":
    import boto3

    cred = Credential.from_s3_json(
        "sanhe-credential", "db/elephant-dupe-remove.json",
        aws_profile="sanhe",
    )
    print(cred)
