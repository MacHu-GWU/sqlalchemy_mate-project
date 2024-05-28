# -*- coding: utf-8 -*-

from sqlalchemy_mate.patterns.large_binary_column.helpers import (
    get_md5,
    get_sha256,
    b64encode_str,
    b64decode_str,
    encode_pk,
    execute_write,
)


def test_get_md5():
    assert len(get_md5(b"hello world")) == 32


def test_get_sha256():
    assert len(get_sha256(b"hello world")) == 64


def test_b64_encode_decode_str():
    url = "http://www.example.com"
    b64 = b64encode_str(url)
    assert b64decode_str(b64) == url


def test_encode_pk():
    assert encode_pk("a", is_pk_url_safe=True) == "a"
    assert encode_pk(("a", "b"), is_pk_url_safe=True) == "a/b"
    assert ":" not in encode_pk("a", is_pk_url_safe=False)
    assert ":" not in encode_pk(("a", "b"), is_pk_url_safe=False)


def test_execute_write():
    def write_function():
        pass

    def return_true():
        return True

    def return_false():
        return False

    write_kwargs = {}

    assert (
        execute_write(
            write_function=write_function,
            write_kwargs=write_kwargs,
            check_exists_function=None,
            check_exists_kwargs=None,
        )
        is True
    )
    assert (
        execute_write(
            write_function=write_function,
            write_kwargs=write_kwargs,
            check_exists_function=return_true,
            check_exists_kwargs={},
        )
        is False
    )
    assert (
        execute_write(
            write_function=write_function,
            write_kwargs=write_kwargs,
            check_exists_function=return_false,
            check_exists_kwargs={},
        )
        is True
    )


if __name__ == "__main__":
    from sqlalchemy_mate.tests.api import run_cov_test

    run_cov_test(
        __file__, "sqlalchemy_mate.patterns.large_binary_column.helpers", preview=False
    )
