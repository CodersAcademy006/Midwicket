"""
Tests for pypitch.serve.auth — API key verification, JWT, and password hashing.

Coverage target: raise auth.py from 31% to >= 70%.
"""

import os
import pytest
from unittest.mock import patch


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_env(monkeypatch):
    """Ensure a clean env state for every auth test."""
    monkeypatch.setenv("PYPITCH_API_KEY_REQUIRED", "false")
    monkeypatch.delenv("PYPITCH_API_KEYS", raising=False)
    yield


# ── verify_api_key ────────────────────────────────────────────────────────────

class TestVerifyApiKey:
    """Unit tests for verify_api_key() — no HTTP server required."""

    def _make_request(self, headers: dict):
        """Build a minimal mock Request-like object."""
        from unittest.mock import Mock
        req = Mock()
        req.headers = headers
        return req

    def test_skipped_when_not_required(self, monkeypatch):
        """When API_KEY_REQUIRED is false, verify_api_key returns True."""
        monkeypatch.setenv("PYPITCH_API_KEY_REQUIRED", "false")
        # Re-import to pick up env change
        import importlib
        import pypitch.serve.auth as auth_mod
        import pypitch.config as cfg_mod
        monkeypatch.setattr(cfg_mod, "API_KEY_REQUIRED", False)

        from pypitch.serve.auth import verify_api_key
        result = verify_api_key(self._make_request({}), credentials=None)
        assert result is True

    def test_bearer_token_accepted(self, monkeypatch):
        """Valid Bearer token is accepted."""
        monkeypatch.setenv("PYPITCH_API_KEYS", "my-secret-key")
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        from fastapi.security import HTTPAuthorizationCredentials
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="my-secret-key")
        result = auth_mod.verify_api_key(self._make_request({}), credentials=creds)
        assert result is True

    def test_x_api_key_header_accepted(self, monkeypatch):
        """Valid X-API-Key header is accepted when no Bearer token present."""
        monkeypatch.setenv("PYPITCH_API_KEYS", "header-key")
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        req = self._make_request({"X-API-Key": "header-key"})
        result = auth_mod.verify_api_key(req, credentials=None)
        assert result is True

    def test_missing_key_raises_401(self, monkeypatch):
        """No token and no X-API-Key header raises 401."""
        monkeypatch.setenv("PYPITCH_API_KEYS", "some-key")
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc_info:
            auth_mod.verify_api_key(self._make_request({}), credentials=None)
        assert exc_info.value.status_code == 401

    def test_wrong_key_raises_401(self, monkeypatch):
        """Wrong API key raises 401."""
        monkeypatch.setenv("PYPITCH_API_KEYS", "correct-key")
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        from fastapi.security import HTTPAuthorizationCredentials
        from fastapi import HTTPException
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="wrong-key")
        with pytest.raises(HTTPException) as exc_info:
            auth_mod.verify_api_key(self._make_request({}), credentials=creds)
        assert exc_info.value.status_code == 401

    def test_no_keys_configured_raises_503(self, monkeypatch):
        """When auth is required but PYPITCH_API_KEYS is empty, returns 503."""
        monkeypatch.setenv("PYPITCH_API_KEYS", "")
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", True)

        from fastapi.security import HTTPAuthorizationCredentials
        from fastapi import HTTPException
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="any-key")
        with pytest.raises(HTTPException) as exc_info:
            auth_mod.verify_api_key(self._make_request({}), credentials=creds)
        assert exc_info.value.status_code == 503


# ── generate_api_key ──────────────────────────────────────────────────────────

class TestGenerateApiKey:
    def test_returns_url_safe_string(self):
        from pypitch.serve.auth import generate_api_key
        key = generate_api_key()
        assert isinstance(key, str)
        assert len(key) >= 32

    def test_keys_are_unique(self):
        from pypitch.serve.auth import generate_api_key
        keys = {generate_api_key() for _ in range(10)}
        assert len(keys) == 10


# ── password hashing ──────────────────────────────────────────────────────────

class TestPasswordHashing:
    def test_hash_and_verify_roundtrip(self):
        """hash_password + verify_password round-trip succeeds."""
        try:
            from pypitch.serve.auth import hash_password, verify_password, HAS_PASSLIB
        except RuntimeError:
            pytest.skip("passlib not installed")
        if not HAS_PASSLIB:
            pytest.skip("passlib not installed")

        try:
            hashed = hash_password("my-secret-pw")
        except Exception as exc:
            pytest.skip(f"bcrypt backend error (version incompatibility): {exc}")

        assert hashed != "my-secret-pw"
        assert verify_password("my-secret-pw", hashed) is True
        assert verify_password("wrong-pw", hashed) is False

    def test_hash_without_passlib_raises(self, monkeypatch):
        """hash_password raises RuntimeError when passlib is missing."""
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "HAS_PASSLIB", False)

        with pytest.raises(RuntimeError, match="passlib"):
            auth_mod.hash_password("pw")

    def test_verify_without_passlib_raises(self, monkeypatch):
        """verify_password raises RuntimeError when passlib is missing."""
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "HAS_PASSLIB", False)

        with pytest.raises(RuntimeError, match="passlib"):
            auth_mod.verify_password("pw", "hash")


# ── JWT ───────────────────────────────────────────────────────────────────────

class TestJWT:
    def test_create_and_decode_roundtrip(self, monkeypatch):
        """JWT round-trip: create → decode returns original payload."""
        try:
            from pypitch.serve.auth import create_access_token, decode_access_token, HAS_JWT
        except ImportError:
            pytest.skip("python-jose not installed")
        if not HAS_JWT:
            pytest.skip("python-jose not installed")

        monkeypatch.setenv("PYPITCH_SECRET_KEY", "jwt-test-secret-key")

        token = create_access_token({"sub": "user123"})
        payload = decode_access_token(token)
        assert payload["sub"] == "user123"

    def test_expired_token_raises_401(self, monkeypatch):
        """An expired JWT raises HTTPException(401)."""
        try:
            from pypitch.serve.auth import create_access_token, decode_access_token, HAS_JWT
        except ImportError:
            pytest.skip("python-jose not installed")
        if not HAS_JWT:
            pytest.skip("python-jose not installed")

        from datetime import timedelta
        from fastapi import HTTPException

        monkeypatch.setenv("PYPITCH_SECRET_KEY", "jwt-test-secret-key")

        token = create_access_token({"sub": "x"}, expires_delta=timedelta(seconds=-1))
        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(token)
        assert exc_info.value.status_code == 401

    def test_create_without_jose_raises(self, monkeypatch):
        """create_access_token raises RuntimeError when python-jose is missing."""
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "HAS_JWT", False)

        with pytest.raises(RuntimeError, match="python-jose"):
            auth_mod.create_access_token({"sub": "x"})

    def test_decode_without_jose_raises(self, monkeypatch):
        """decode_access_token raises RuntimeError when python-jose is missing."""
        import pypitch.serve.auth as auth_mod
        monkeypatch.setattr(auth_mod, "HAS_JWT", False)

        with pytest.raises(RuntimeError, match="python-jose"):
            auth_mod.decode_access_token("any-token")
