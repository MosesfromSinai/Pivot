import os
from decimal import Decimal

import pymysql
import pymysql.cursors


class DatabaseError(Exception):
    """MySQL is unreachable or a query failed."""


def get_connection():
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3307")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "pivot"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )


def query(sql, params=None):
    # Pass values through params with %s placeholders, never by formatting sql
    try:
        conn = get_connection()
    except pymysql.MySQLError as e:
        raise DatabaseError(f"Can't connect to MySQL: {e}") from e
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except pymysql.MySQLError as e:
        raise DatabaseError(f"Query failed: {e}") from e
    finally:
        conn.close()
    # SUM/AVG come back as Decimal, which Jinja's tojson can't serialize
    return [
        {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]
