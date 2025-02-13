import psycopg2
from psycopg2.pool import SimpleConnectionPool
import time
from settings import DB_CONFIG, CHUNK_SIZE

DB_CONN_STRING = (
    f"dbname={DB_CONFIG['dbname']} user={DB_CONFIG['user']} "
    f"password={DB_CONFIG['password']} host={DB_CONFIG['host']} "
    f"port={DB_CONFIG['port']} sslmode=require"
)

# Ensure we do not exceed Render's DB connection limits
MAX_CONNECTIONS = 5

# Establish a connection pool with error handling
try:
    connection_pool = SimpleConnectionPool(
        minconn=1, maxconn=MAX_CONNECTIONS, dsn=DB_CONN_STRING
    )
except psycopg2.OperationalError as e:
    print(f"⚠️ Failed to create connection pool: {e}")
    exit(1)


def get_connection(retries=3, delay=5):
    """Retrieve a connection with automatic retries on failure."""
    for attempt in range(retries):
        try:
            conn = connection_pool.getconn()
            if conn.closed:
                raise psycopg2.OperationalError("Connection was closed")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Database connection failed (Attempt {attempt+1}): {e}")
            time.sleep(delay)
    raise Exception("Database connection failed after multiple attempts")


def release_connection(conn):
    """Safely return connection to the pool or close it if invalid."""
    if conn:
        if conn.closed:
            print("Skipping release: Connection was already closed.")
        else:
            connection_pool.putconn(conn)


def fetch_data_chunk(last_id=0, limit=CHUNK_SIZE):
    """Fetch data efficiently without causing connection issues."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        query = f"""
            SELECT id, workspace_id, request_start_time, request_end_time, 
                   price, tokens, engine, tags, analytics_metadata, synced
            FROM analytics_data
            WHERE id > {last_id}
            ORDER BY id ASC
            LIMIT {limit}
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except psycopg2.OperationalError as e:
        print(f"Error fetching data: {e}")
        return []
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    print("Starting database sync...")
    try:
        records = fetch_data_chunk()
        print(f"Fetched {len(records)} records.")
    except Exception as e:
        print(f"Sync failed: {e}")
