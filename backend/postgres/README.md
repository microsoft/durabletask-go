# Postgres Backend

This directory contains code that enables a postgres backend for the Durable Task runtime.

## Testing

By default, the postgres tests are skipped. To run the tests, set the environment variable `POSTGRES_ENABLED` to `true` before running the tests and have a postgres server running on `localhost:5432` with a database named `postgres` and a user `postgres` with password `postgres`.
