#!/usr/bin/env python3
import urllib.request
import sys

def check_health():
    try:
        with urllib.request.urlopen('http://localhost:8000/health', timeout=5) as response:
            if response.getcode() == 200:
                sys.exit(0)
            else:
                sys.exit(1)
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    check_health()
