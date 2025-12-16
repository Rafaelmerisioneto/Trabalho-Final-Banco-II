import psycopg2
import psycopg2.extras
import os

def get_postgres_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=5432,
    )


def query(sql, params=None):
    conn = get_postgres_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def execute(sql, params=None, returning=False):
    conn = get_postgres_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        if returning:
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        conn.commit()
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def fetch_postgres_data():
    return query("SELECT * FROM public.produtos;")  # 👈 MUITO IMPORTANTE
