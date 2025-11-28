import os
import psycopg

DSN = os.getenv("POSTGRES_DSN", "dbname=app user=postgres password=postgres host=postgres port=5432")


def get_conn():
    return psycopg.connect(DSN, autocommit=True)
