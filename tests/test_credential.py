# -*- coding: utf-8 -*-

import pytest
from pytest import raises
import os
import json
from sqlalchemy_mate.credential import Credential, EngineCreator

json_file = os.path.join(os.path.dirname(__file__), ".db.json")
with open(json_file, "wb") as f:
    f.write(
        json.dumps({
            "mydb": dict(host="host", port=1234, database="dev",
                         username="user", password="pass")
        }).encode("utf-8")
    )


class TestCredential(object):
    def test_uri(self):
        cred = Credential(host="host", port=1234, database="dev", username="user", password="pass")
        assert cred.uri == "user:pass@host:1234/dev"

        cred = Credential(host="host", port=1234, database="dev", username="user")
        assert cred.uri == "user@host:1234/dev"

        cred = Credential(host="host", database="dev", username="user", password="pass")
        assert cred.uri == "user:pass@host/dev"

        cred = Credential(host="host", database="dev", username="user")
        assert cred.uri == "user@host/dev"

    def test_from_json_data(self):
        cred = Credential.from_json(json_file, json_path="mydb")
        assert cred.uri == "user:pass@host:1234/dev"

        cred.to_dict()

    def test_from_env(self):
        os.environ["DB_HOST"] = "host"
        os.environ["DB_PORT"] = "1234"
        os.environ["DB_DATABASE"] = "dev"
        os.environ["DB_USERNAME"] = "user"
        os.environ["DB_PASSWORD"] = "pass"
        cred = Credential.from_env(prefix="DB")
        assert cred.uri == "user:pass@host:1234/dev"
        cred = Credential.from_env(prefix="DB_")
        assert cred.uri == "user:pass@host:1234/dev"

        with raises(ValueError):
            Credential.from_env(prefix="db")

        with raises(ValueError):
            Credential.from_env(prefix="VAR1")

    def test_validate_key_mapping(self):
        with raises(ValueError):
            Credential._validate_key_mapping({})

        with raises(ValueError):
            Credential._validate_key_mapping(dict(a=1, b=2))

        Credential._validate_key_mapping(None)
        Credential._validate_key_mapping(
            dict(
                host="host", port="port", database="db",
                username="user", password="pass"
            )
        )

    def test_transform(self):
        data = {"host": 1, "port": 2, "db": 3, "user": 4, "pass": 5}
        new_data = Credential._transform(
            data, dict(
                host="host", port="port", database="db",
                username="user", password="pass"
            )
        )
        assert new_data == dict(host=1, port=2, database=3, username=4, password=5)


class TestEngineCreator(object):
    def test(self):
        ec = EngineCreator.from_json(json_file, "mydb")
        ec.create_sqlite()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
