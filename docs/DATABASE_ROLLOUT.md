# ECHO Managed Database Rollout

ECHO supports managed PostgreSQL without removing its SQLite fallback.

## Selection order

The first configured value wins:

1. `ECHO_DATABASE_URL`
2. `DATABASE_URL`
3. `POSTGRES_URL`
4. `POSTGRES_URL_NON_POOLING`
5. `SUPABASE_DB_URL`
6. Legacy `ECHO_DB`
7. SQLite fallback (`/tmp/echo.db` on Vercel, `echo_data/echo.db` locally)

PostgreSQL URLs are normalized to SQLAlchemy's psycopg 3 driver. Credentials are never included in health responses or logs.

## Isolation

Managed PostgreSQL connections use the schema configured by `ECHO_DB_SCHEMA`, which defaults to `echo`. The connection search path is `echo,public`, preventing collisions with unrelated `public.conversations`, `public.messages`, or job tables.

## Safe rollout

1. Provision the isolated `echo` schema and tables.
2. Deploy the code while no managed URL is attached; SQLite remains active.
3. Attach a pooled PostgreSQL URL to Preview only.
4. Run conversation, job, receipt, restart, and integrity smoke tests.
5. Attach the same integration to Production.
6. Verify rows persist across separate function invocations and deployments.
7. Keep authentication in `shadow` until persistence is proven.

Rollback requires removing the managed database variable. ECHO immediately returns to its SQLite fallback without a code rollback.

## Access control

The `echo` schema is intended for server-side database connections only. Do not expose it through Supabase client APIs. Explicitly revoke schema and table access from `anon` and `authenticated`. Row Level Security should only be enabled together with tested policies; enabling RLS without policies can block application access.
