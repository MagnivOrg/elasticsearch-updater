import psycopg2
from settings import DB_CONFIG, CHUNK_SIZE

DB_CONN_STRING = (
    f"dbname={DB_CONFIG['dbname']} user={DB_CONFIG['user']} "
    f"password={DB_CONFIG['password']} host={DB_CONFIG['host']} "
    f"port={DB_CONFIG['port']} sslmode=require"
)

def get_connection():
    """Establish a new PostgreSQL connection."""
    return psycopg2.connect(DB_CONN_STRING)

def release_connection(conn):
    """Close the PostgreSQL connection."""
    conn.close()

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
