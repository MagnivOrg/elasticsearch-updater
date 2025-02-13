from datetime import datetime

import psycopg2
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from settings import DB_CONFIG, CHUNK_SIZE
from models import AnalyticsData

# Initialize SQLAlchemy session
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def fetch_data_chunk(last_id=0, limit=CHUNK_SIZE):
    """Fetch data in an optimized way without full table scans."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(name="data_cursor")

    query = f"""
       WITH request_data AS (
        SELECT 
            rl.id AS request_log_id,
            rl.workspace_id,
            rl.prompt_id,
            rl.prompt_name,
            rl.request_start_time,
            rl.request_end_time,
            rl.price,
            rl.tokens,
            rl.engine
        FROM request_logs AS rl
        WHERE rl.id > {last_id}  -- No OFFSET to avoid slow performance
        ORDER BY rl.id ASC
        LIMIT {limit}
    )
    SELECT
        r.*,
        COALESCE(tags.tag_names, ARRAY[]::TEXT[]) AS tags,
        COALESCE(metadata.meta_data, CAST('{{}}' AS jsonb)) AS analytics_metadata
    FROM request_data r
    LEFT JOIN (
        SELECT rt.request_id, ARRAY_AGG(DISTINCT t.name) AS tag_names
        FROM requests_tags rt
        JOIN tags t ON rt.tag_id = t.id
        GROUP BY rt.request_id
    ) AS tags ON r.request_log_id = tags.request_id
    LEFT JOIN (
        SELECT mv.request_id, JSONB_OBJECT_AGG(mf.name, mv.value)::jsonb AS meta_data
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
                "tags": row[9] if row[9] else [],
                "analytics_metadata": row[10] if row[10] else {},
                "synced": False,
            }
            for row in rows
        ]

    cursor.close()
    conn.close()


def update_analytics_data():
    """Efficiently update analytics_data table with historical data."""
    session = Session()
    try:
        last_id = 0  # Track last processed ID
        while True:
            data_chunk = list(fetch_data_chunk(last_id, CHUNK_SIZE))
            if not data_chunk:
                break

            for data in data_chunk:
                existing_entry = session.query(AnalyticsData).filter_by(request_log_id=data["request_log_id"]).first()

                if existing_entry:
                    # Update existing analytics data
                    for key, value in data.items():
                        setattr(existing_entry, key, value)
                else:
                    # Insert new analytics entry
                    analytics_entry = AnalyticsData(**data)
                    session.add(analytics_entry)

                last_id = data["request_log_id"]  # Update last processed ID

            session.commit()
            print(f"Processed {len(data_chunk)} records into analytics_data.")

    except Exception as e:
        session.rollback()
        print(f"Error processing data: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    update_analytics_data()
