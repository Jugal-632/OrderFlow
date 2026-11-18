# Synthetic test credentials

Credentials and signing material inside `src/test/` and frontend test fixtures are synthetic, disposable test data. They are not deployment credentials and must never be reused for running services. Local and CI Compose credentials are generated into ignored `.env` by `scripts/local_env.py`; production credentials are supplied through environment configuration. The benchmark generates its own random temporary account password.

The generator preserves existing local values because changing a password does not update an existing PostgreSQL volume or already-created Auth accounts. Use a new isolated Compose project for a fresh environment; do not delete existing volumes to rotate credentials.
