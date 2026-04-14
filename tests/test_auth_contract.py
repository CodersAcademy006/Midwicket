"""Tests for API auth/header contract consistency."""

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from starlette.requests import Request

from pypitch.client import PyPitchClient
from pypitch.serve.auth import verify_api_key
from pypitch.serve.rate_limit import get_client_key


def _request_with_headers(headers: dict[str, str]) -> Request:
    raw_headers = [(k.lower().encode("utf-8"), v.encode("utf-8")) for k, v in headers.items()]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope)


def test_verify_api_key_accepts_bearer_token(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", True)
    monkeypatch.setenv("PYPITCH_API_KEYS", "abc123")

    req = _request_with_headers({"Authorization": "Bearer abc123"})
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="abc123")

    assert verify_api_key(req, creds) is True


def test_verify_api_key_accepts_legacy_x_api_key(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", True)
    monkeypatch.setenv("PYPITCH_API_KEYS", "legacy-key")

    req = _request_with_headers({"X-API-Key": "legacy-key"})

    assert verify_api_key(req, None) is True


def test_verify_api_key_rejects_missing_token(monkeypatch):
    monkeypatch.setattr("pypitch.serve.auth.API_KEY_REQUIRED", True)
    monkeypatch.setenv("PYPITCH_API_KEYS", "abc123")

    req = _request_with_headers({})

    try:
        verify_api_key(req, None)
        assert False, "Expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 401


def test_rate_limit_client_key_prefers_bearer():
    req = _request_with_headers({
        "Authorization": "Bearer token-1",
        "X-API-Key": "legacy-token",
    })

    assert get_client_key(req) == "api_key:token-1"


def test_client_sets_bearer_and_legacy_headers():
    client = PyPitchClient(api_key="token-xyz")

    assert client.session.headers["Authorization"] == "Bearer token-xyz"
    assert client.session.headers["X-API-Key"] == "token-xyz"
