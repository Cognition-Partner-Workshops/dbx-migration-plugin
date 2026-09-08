"""One-shot initial load for the rehearsal: SELECT from the SQL Server (Sybase stand-in) source,
COPY into the Lakebase-shaped Postgres target, then restart every identity sequence above the
loaded max so tier 7's sequence headroom check has something real to grade.

Source access is read-only SELECT. Secrets are env-var NAMES:
  python load_target.py --source-dsn-secret REHEARSAL_SOURCE_ODBC \
      --target-secret LAKEBASE_MIGRATION_DSN --source-schema raw --target-schema loan_servicing
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg
import pyodbc

# parent -> child order so foreign keys hold during the load
TABLES = ("borrowers", "loans", "payments", "escrow_accounts", "loan_modifications")


def _secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"secret '{name}' not found in environment; pass secrets by name only")
    return value


def _ident(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise SystemExit(f"unsafe identifier {name!r}")
    return name


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source-dsn-secret", required=True)
    p.add_argument("--target-secret", required=True)
    p.add_argument("--source-schema", default="raw")
    p.add_argument("--target-schema", default="loan_servicing")
    args = p.parse_args()
    s_schema, t_schema = _ident(args.source_schema), _ident(args.target_schema)

    src = pyodbc.connect(_secret(args.source_dsn_secret), readonly=True)
    tgt = psycopg.connect(_secret(args.target_secret))
    with tgt, tgt.cursor() as tcur:
        for table in TABLES:
            tcur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (t_schema, table))
            cols = [r[0] for r in tcur.fetchall()]
            if not cols:
                raise SystemExit(f"target {t_schema}.{table} does not exist; apply target_ddl.sql first")
            scur = src.cursor()
            scur.execute(f"SELECT {', '.join(cols)} FROM {s_schema}.{table}")
            tcur.execute(f'TRUNCATE "{t_schema}"."{table}" CASCADE')
            n = 0
            with tcur.copy(f'COPY "{t_schema}"."{table}" ({", ".join(cols)}) FROM STDIN') as copy:
                for row in scur:
                    copy.write_row(tuple(row))
                    n += 1
            tcur.execute(
                f'SELECT setval(pg_get_serial_sequence(%s, %s), '
                f'GREATEST(COALESCE((SELECT MAX("{cols[0]}") FROM "{t_schema}"."{table}"), 0), 1), '
                f'(SELECT COUNT(*) > 0 FROM "{t_schema}"."{table}"))',
                (f'"{t_schema}"."{table}"', cols[0]))
            print(f"{s_schema}.{table} -> {t_schema}.{table}: {n} rows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
