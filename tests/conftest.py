import os

# Configure environment for tests
# This must be done before importing pypitch modules
os.environ["PYPITCH_ENV"] = "development"
os.environ["PYPITCH_SECRET_KEY"] = "test-secret-key-for-pytest"
