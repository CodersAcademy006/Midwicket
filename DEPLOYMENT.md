# Midwicket Deployment Guide

This guide explains how to deploy and publish new versions of the `midwicket` SDK to the official Python Package Index (PyPI). 

We use **PyPI Trusted Publishing (OIDC)** with GitHub Actions. This is the most secure method because it requires **no API tokens or passwords**—PyPI securely trusts the GitHub repository itself.

## One-time Setup: Configure PyPI Trusted Publishing

You only need to do this once per repository:

1. Log in to your PyPI account at [pypi.org/manage/account/publishing/](https://pypi.org/manage/account/publishing/).
2. Scroll to the **"Add a new publisher"** section and select **GitHub**.
3. Fill in the following details:
   - **Publisher name:** `Midwicket GitHub Actions` (or anything descriptive)
   - **Repository owner:** `CodersAcademy006`
   - **Repository name:** `Midwicket`
   - **Workflow name:** `publish.yml`
   - **Environment name:** *(leave this blank)*
4. Click **Add**. PyPI will now accept packages uploaded by this repository's GitHub Actions.

## How to Publish a New Version

Once the one-time setup is complete, deploying a new version is entirely automated:

1. **Update the version number** in `pyproject.toml` (e.g., from `0.1.0` to `0.1.1`) and merge that change to the `main` branch.
2. Go to your repository on GitHub.
3. Click on **Releases** (on the right sidebar) -> **Draft a new release**.
4. Choose a tag matching your version (e.g., `v0.1.1`).
5. Enter a release title and description.
6. Click **Publish Release**.

That's it! GitHub Actions will automatically:
- Check out the code.
- Setup Python 3.11.
- Build the binary wheels (`.whl`) and source tarballs (`.tar.gz`) using `build`.
- Securely authenticate via OIDC and upload them directly to PyPI.

You can monitor the progress in the **Actions** tab on GitHub.

---

## (Fallback) Manual Publishing

If for some reason you cannot use GitHub Actions and need to publish manually from your local machine, follow these steps. 

**Note**: You will need a PyPI API token for this method.

1. **Install build tools:**
   ```bash
   pip install --upgrade build twine
   ```

2. **Build the distribution files:**
   ```bash
   python -m build
   ```
   *This will generate `.tar.gz` and `.whl` files in the `dist/` directory.*

3. **Upload using Twine:**
   ```bash
   twine upload dist/*
   ```
   *When prompted, use `__token__` as the username, and paste your PyPI API token as the password.*
