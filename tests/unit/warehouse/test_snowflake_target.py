from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from millrace.settings import Settings
from millrace.warehouse.snowflake_target import credential_kwargs, load_private_key


def test_plain_password_is_used_when_no_authenticator_or_key_is_set() -> None:
    settings = _settings(snowflake_password="secret")

    assert credential_kwargs(settings) == {"password": "secret"}


def test_programmatic_access_token_uses_the_token_field_not_password() -> None:
    settings = _settings(
        snowflake_password="pat-value",
        snowflake_authenticator="PROGRAMMATIC_ACCESS_TOKEN",
    )

    assert credential_kwargs(settings) == {
        "token": "pat-value",
        "authenticator": "PROGRAMMATIC_ACCESS_TOKEN",
    }


def test_other_authenticator_values_pass_through_with_the_password() -> None:
    settings = _settings(snowflake_password="secret", snowflake_authenticator="externalbrowser")

    assert credential_kwargs(settings) == {
        "password": "secret",
        "authenticator": "externalbrowser",
    }


def test_private_key_path_takes_priority_over_password(tmp_path: Path) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path = tmp_path / "rsa_key.p8"
    key_path.write_bytes(pem)
    settings = _settings(snowflake_password="unused", snowflake_private_key_path=str(key_path))

    kwargs = credential_kwargs(settings)

    assert set(kwargs) == {"private_key"}


def test_load_private_key_produces_der_bytes(tmp_path: Path) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path = tmp_path / "rsa_key.p8"
    key_path.write_bytes(pem)
    settings = _settings(snowflake_private_key_path=str(key_path))

    der = load_private_key(settings)

    reloaded = serialization.load_der_private_key(der, password=None)
    assert isinstance(reloaded, rsa.RSAPrivateKey)
    assert reloaded.key_size == 2048


def test_load_private_key_expands_a_leading_tilde(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    monkeypatch.setenv("HOME", str(tmp_path))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    (tmp_path / "rsa_key.p8").write_bytes(pem)
    settings = _settings(snowflake_private_key_path="~/rsa_key.p8")

    der = load_private_key(settings)

    reloaded = serialization.load_der_private_key(der, password=None)
    assert isinstance(reloaded, rsa.RSAPrivateKey)


def test_load_private_key_with_wrong_passphrase_fails_closed(tmp_path: Path) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(b"correct-passphrase"),
    )
    key_path = tmp_path / "rsa_key.p8"
    key_path.write_bytes(pem)
    settings = _settings(
        snowflake_private_key_path=str(key_path),
        snowflake_private_key_passphrase="wrong-passphrase",
    )

    with pytest.raises(ValueError, match=r"[Pp]assword|[Dd]ecrypt"):
        load_private_key(settings)


def _settings(
    *,
    snowflake_password: str = "",
    snowflake_authenticator: str = "",
    snowflake_private_key_path: str = "",
    snowflake_private_key_passphrase: str = "",
) -> Settings:
    return Settings(
        snowflake_account="account",
        snowflake_user="user",
        snowflake_warehouse="warehouse",
        snowflake_database="database",
        snowflake_password=SecretStr(snowflake_password),
        snowflake_authenticator=snowflake_authenticator,
        snowflake_private_key_path=snowflake_private_key_path,
        snowflake_private_key_passphrase=SecretStr(snowflake_private_key_passphrase),
    )
