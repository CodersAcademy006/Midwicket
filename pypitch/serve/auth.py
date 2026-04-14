"""
Security and authentication utilities for PyPitch API.
"""

import secrets
from typing import Optional
from fastapi import HTTPException, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timedelta, timezone

# JWT handling (conditional import)
try:
    from jose import jwt
    HAS_JWT = True
except ImportError:
    jwt = None
    HAS_JWT = False

from pypitch.config import get_secret_key, API_KEY_REQUIRED

# Password hashing (conditional import)
try:
    from passlib.context import CryptContext
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    HAS_PASSLIB = True
except ImportError:
    pwd_context = None
    HAS_PASSLIB = False

# API Key authentication
security = HTTPBearer(auto_error=False)

def verify_api_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> bool:
    """
    Verify API key against configured valid keys.

    Keys are loaded from the ``PYPITCH_API_KEYS`` env var (comma-separated).
    Accepted request formats:
    - ``Authorization: Bearer <token>`` (preferred)
    - ``X-API-Key: <token>`` (backward compatibility)
    When ``API_KEY_REQUIRED`` is ``False`` the check is skipped entirely.
    """
    if not API_KEY_REQUIRED:
        return True

    token: Optional[str] = None
    if credentials and credentials.credentials:
        token = credentials.credentials
    else:
        legacy_token = request.headers.get("X-API-Key")
        if legacy_token:
            token = legacy_token.strip()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    import hmac
    import os
    valid_keys = [
        k.strip()
        for k in os.getenv("PYPITCH_API_KEYS", "").split(",")
        if k.strip()
    ]
    if not valid_keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Server misconfiguration: no API keys configured",
        )

    # Constant-time comparison to prevent timing attacks
    if not any(hmac.compare_digest(token, k) for k in valid_keys):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    return True

def hash_password(password: str) -> str:
    """Hash a password."""
    if not HAS_PASSLIB:
        raise RuntimeError("Password hashing requires 'passlib' package. Install with: pip install passlib[bcrypt]")
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    if not HAS_PASSLIB:
        raise RuntimeError("Password verification requires 'passlib' package. Install with: pip install passlib[bcrypt]")
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token."""
    if not HAS_JWT:
        raise RuntimeError("JWT token creation requires 'python-jose' package. Install with: pip install python-jose[cryptography]")
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, get_secret_key(), algorithm="HS256")
    return encoded_jwt

def decode_access_token(token: str):
    """Decode and verify JWT token."""
    if not HAS_JWT:
        raise RuntimeError("JWT token decoding requires 'python-jose' package. Install with: pip install python-jose[cryptography]")
    try:
        payload = jwt.decode(token, get_secret_key(), algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def generate_api_key() -> str:
    """Generate a secure API key."""
    return secrets.token_urlsafe(32)