import os

import psycopg

DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/app")


def get_conn():
    return psycopg.connect(DSN, autocommit=True)
