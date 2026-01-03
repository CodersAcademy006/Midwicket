"""
Security and authentication utilities for PyPitch API.
"""

import os
import secrets
from typing import Optional
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timedelta

# JWT handling (conditional import)
try:
    import jwt
    HAS_JWT = True
except ImportError:
    jwt = None
    HAS_JWT = False

from pypitch.config import SECRET_KEY, API_KEY_REQUIRED

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

def verify_api_key(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> bool:
    """
    Verify API key if required.
    In production, this should check against a database of valid keys.
    """
    if not API_KEY_REQUIRED:
        return True

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # For demo purposes, accept any non-empty key
    # In production, validate against database
    if not credentials.credentials or len(credentials.credentials) < 10:
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
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
    return encoded_jwt

def decode_access_token(token: str):
    """Decode and verify JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def generate_api_key() -> str:
    """Generate a secure API key."""
    return secrets.token_urlsafe(32)