import time
from datetime import datetime

import psycopg2
import json
from settings import DB_CONFIG, CHUNK_SIZE


def get_connection():
    """Establish a direct connection to PostgreSQL."""
    return psycopg2.connect(
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        sslmode="require"
    )


def get_last_synced_id():
    """Get the last synced request_log_id from analytics_data."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(request_log_id) FROM analytics_data")
    last_id = cursor.fetchone()[0] or 0

    cursor.close()
    conn.close()
    return last_id


def fetch_data_chunk(last_id=0, limit=CHUNK_SIZE):
    """Fetch relevant data from request_logs, metadata, and tags for analytics processing."""
    conn = get_connection()
    cursor = conn.cursor(name="data_cursor")

    query = f"""
        WITH request_data AS (
            SELECT 
                rl.id AS request_log_id,
                rl.workspace_id,
                rl.prompt_id,
                p.prompt_name,
                rl.request_start_time,
                rl.request_end_time,
                rl.price,
                rl.tokens,
                rl.engine
            FROM request_logs AS rl
            LEFT JOIN prompt_registry p ON rl.prompt_id = p.id
            WHERE rl.id > {last_id}  -- Fetch only new records
            ORDER BY rl.id ASC
            LIMIT {limit}
        )
        SELECT
            r.*,
            COALESCE(tags.tag_names, '[]'::jsonb) AS tags,  -- Ensure JSONB format
            COALESCE(metadata.meta_data, '{{}}'::jsonb) AS analytics_metadata
        FROM request_data r
        LEFT JOIN (
            SELECT rt.request_id, JSONB_AGG(DISTINCT t.name) AS tag_names
            FROM requests_tags rt
            JOIN tags t ON rt.tag_id = t.id
            GROUP BY rt.request_id
        ) AS tags ON r.request_log_id = tags.request_id
        LEFT JOIN (
            SELECT mv.request_id, JSONB_OBJECT_AGG(mf.name, mv.value) AS meta_data
            FROM metadata_value mv
            JOIN metadata_field mf ON mv.metadata_field_id = mf.id
            GROUP BY mv.request_id
        ) AS metadata ON r.request_log_id = metadata.request_id;
    """

    cursor.execute(query)

    while True:
        rows = cursor.fetchmany(size=CHUNK_SIZE)
        if not rows:
            break

        yield [
            {
                "request_log_id": row[0],
                "workspace_id": row[1],
                "prompt_id": row[2],
                "prompt_name": row[3],
                "request_start_time": row[4].isoformat() if isinstance(row[4], datetime) else row[4],
                "request_end_time": row[5].isoformat() if isinstance(row[5], datetime) else row[5],
                "price": row[6],
                "tokens": row[7],
                "engine": row[8],
                "tags": row[9],
                "analytics_metadata": row[10],
                "synced": False,
            }
            for row in rows
        ]

    cursor.close()
    conn.close()


def update_analytics_data():
    """Insert fetched data into analytics_data with deduplication."""
    conn = get_connection()
    cursor = conn.cursor()
    last_id = get_last_synced_id()

    records_synced = 0

    try:
        while True:
            data_chunk = [item for batch in fetch_data_chunk(last_id, CHUNK_SIZE) for item in batch]
            if not data_chunk:
                break

            for data in data_chunk:
                request_log_id = data["request_log_id"]
                workspace_id = data["workspace_id"]
                prompt_id = data["prompt_id"]
                prompt_name = data["prompt_name"]
                request_start_time = data["request_start_time"]
                request_end_time = data["request_end_time"]
                price = data["price"]
                tokens = data["tokens"]
                engine = data["engine"]
                tags = json.dumps(data["tags"])
                analytics_metadata = json.dumps(data["analytics_metadata"])
                synced = False

                cursor.execute(
                    """
                    INSERT INTO analytics_data (
                        request_log_id, workspace_id, prompt_id, prompt_name, 
                        request_start_time, request_end_time, price, tokens, 
                        engine, tags, analytics_metadata, synced
                    ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                    ON CONFLICT (request_log_id) DO UPDATE SET
                        workspace_id = EXCLUDED.workspace_id,
                        prompt_id = EXCLUDED.prompt_id,
                        prompt_name = EXCLUDED.prompt_name,
                        request_start_time = EXCLUDED.request_start_time,
                        request_end_time = EXCLUDED.request_end_time,
                        price = EXCLUDED.price,
                        tokens = EXCLUDED.tokens,
                        engine = EXCLUDED.engine,
                        tags = EXCLUDED.tags,
                        analytics_metadata = EXCLUDED.analytics_metadata,
                        synced = EXCLUDED.synced
                    """,
                    (
                        request_log_id, workspace_id, prompt_id, prompt_name,
                        request_start_time, request_end_time, price, tokens,
                        engine, tags, analytics_metadata, synced
                    )
                )

                last_id = request_log_id
                records_synced += 1

            conn.commit()
            print(f"Processed {len(data_chunk)} records into analytics_data.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")

    finally:
        cursor.close()
        conn.close()

    return records_synced


if __name__ == "__main__":
    print("Starting data sync to analytics_data...")

    while True:
        try:
            records_synced = update_analytics_data()

            if records_synced == 0:
                print("No more records to sync. Retrying in 30 seconds...")
                time.sleep(30)
            else:
                print(f"Processed {records_synced} records. Checking for more...")

        except Exception as e:
            print(f"Error during sync: {e}")
            time.sleep(10)
