# PyPitch API Dockerfile
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pypitch/ ./pypitch/
COPY pyproject.toml .
COPY README.md .

# Create non-root user
RUN useradd --create-home --shell /bin/bash pypitch
RUN chown -R pypitch:pypitch /app
USER pypitch

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; import sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').getcode() == 200 else 1)"

# Run the application
CMD ["python", "-m", "uvicorn", "pypitch.serve.api:PyPitchAPI().app", "--host", "0.0.0.0", "--port", "8000"]