import psycopg2
from elasticsearch import Elasticsearch, helpers
from settings import DB_CONFIG, ELASTICSEARCH_URL, CHUNK_SIZE, INDEX_NAME

def fetch_data_chunk(offset, limit):
    """Fetch a chunk of data from PostgreSQL using a cursor."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(name="data_cursor")

    query = f"""
        SELECT
            rl.id,
            rl.workspace_id,
            rl.request_start_time,
            rl.request_end_time,
            rl.price,
            rl.tokens,
            rl.engine,
            ARRAY_AGG(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tags,
            jsonb_object_agg(mf.name, mv.value) FILTER (WHERE mv.value IS NOT NULL) AS metadata
        FROM request_logs AS rl
        LEFT JOIN requests_tags AS rt ON rl.id = rt.request_id
        LEFT JOIN tags AS t ON rt.tag_id = t.id
        LEFT JOIN metadata_value AS mv ON rl.id = mv.request_id
        LEFT JOIN metadata_field AS mf ON mv.metadata_field_id = mf.id
        GROUP BY rl.id, rl.workspace_id, rl.request_start_time, rl.request_end_time, rl.price, rl.tokens, rl.engine
        OFFSET {offset} LIMIT {limit};
    """
    cursor.execute(query)

    while True:
        rows = cursor.fetchmany(size=CHUNK_SIZE)
        if not rows:
            break

        yield [
            {
                "_op_type": "update",
                "_index": INDEX_NAME,
                "_id": row[0],  # Use id from PostgreSQL as document ID
                "doc": {
                    "workspace_id": row[1],
                    "request_start_time": row[2],
                    "request_end_time": row[3],
                    "price": row[4],
                    "tokens": row[5],
                    "engine": row[6],
                    "tags": row[7] if row[7] else [],
                    "metadata": row[8] if row[8] else {},
                },
                "doc_as_upsert": True  # If not exists, create it
            }
            for row in rows
        ]

    cursor.close()
    conn.close()

def update_elasticsearch():
    """Update Elasticsearch with the latest data in chunks."""
    es = Elasticsearch([ELASTICSEARCH_URL])

    for data_chunk in fetch_data_chunk(0, CHUNK_SIZE):  # Stream data
        helpers.bulk(es, data_chunk)
        print(f"Pushed {len(data_chunk)} records to Elasticsearch.")

    print("Data sync to Elasticsearch completed.")

if __name__ == "__main__":
    update_elasticsearch()
