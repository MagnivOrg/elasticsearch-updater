import json
from datetime import datetime
import psycopg2
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from settings import DB_CONFIG, CHUNK_SIZE

DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

READ_REPLICA_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['read_host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

def fetch_data_chunk(last_id=0, limit=CHUNK_SIZE):
    """Fetch data from the read replica with locking to avoid conflicts."""
    read_conn = psycopg2.connect(READ_REPLICA_URL)
    read_cursor = read_conn.cursor(name="data_cursor")

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
        WHERE rl.id > {last_id}
        ORDER BY rl.id ASC
        LIMIT {limit}
    )
    SELECT
        r.*,
        COALESCE(tags.tag_names, ARRAY[]::TEXT[]) AS tags,
        COALESCE(metadata.meta_data, '{{}}'::jsonb) AS analytics_metadata
    FROM request_data r
    LEFT JOIN (
        SELECT rt.request_id, ARRAY_AGG(DISTINCT t.name) AS tag_names
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
    read_cursor.execute(query)

    while True:
        rows = read_cursor.fetchmany(size=CHUNK_SIZE)
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
                "tags": row[9] if row[9] else [],
                "analytics_metadata": row[10] if row[10] else {},
                "synced": False,
            }
            for row in rows
        ]

    read_cursor.close()
    read_conn.close()


def update_analytics_data():
    """Handle async data processing with batch commits and locking."""
    valid_keys = ["dbname", "user", "password", "host", "port"]
    write_db_config = {key: DB_CONFIG[key] for key in valid_keys if key in DB_CONFIG}

    write_conn = psycopg2.connect(**write_db_config)
    write_cursor = write_conn.cursor()

    last_id = 0
    while True:
        data_chunk = [item for batch in fetch_data_chunk(last_id, CHUNK_SIZE) for item in batch]
        if not data_chunk:
            break

        for item in data_chunk:
            request_log_id = item["request_log_id"]
            workspace_id = item["workspace_id"]
            prompt_id = item["prompt_id"]
            prompt_name = item["prompt_name"]
            request_start_time = item["request_start_time"]
            request_end_time = item["request_end_time"]
            price = item["price"]
            tokens = item["tokens"]
            engine = item["engine"]
            tags = item["tags"]
            analytics_metadata = item["analytics_metadata"]
            synced = item["synced"]

            print(f"Processing Request Log ID: {request_log_id}")

            write_cursor.execute(
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
                    request_log_id,
                    workspace_id,
                    prompt_id,
                    prompt_name,
                    request_start_time,
                    request_end_time,
                    price,
                    tokens,
                    engine,
                    json.dumps(tags),
                    json.dumps(analytics_metadata),
                    synced
                )
            )

            last_id = request_log_id

        write_conn.commit()
        print(f"Processed {len(data_chunk)} records into analytics_data.")

    write_cursor.close()
    write_conn.close()


if __name__ == "__main__":
    update_analytics_data()
