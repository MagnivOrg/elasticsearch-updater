import psycopg2
from elasticsearch import Elasticsearch, helpers

DB_CONFIG = {
    "dbname": "prompt_layer_db",
    "user": "prompt_layer_db_user",
    "password": "NHqotkLZAGWIRB1rQW9QOh6ntmO5lMXH",
    "host": "dpg-cemr30pgp3jlcsi397s0-b",
    "port": "5432",
}

ES_HOST = "https://your-elasticsearch-url.onrender.com"
INDEX_NAME = "request_logs_index"
CHUNK_SIZE = 1000000


def fetch_data_chunk(offset, limit):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

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
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [
        {
            "_id": row[0],
            "_index": INDEX_NAME,
            "_source": {
                "id": row[0],
                "workspace_id": row[1],
                "request_start_time": row[2],
                "request_end_time": row[3],
                "price": row[4],
                "tokens": row[5],
                "engine": row[6],
                "tags": row[7] if row[7] is not None else [],
                "metadata": row[8] if row[8] is not None else {},
            },
        }
        for row in rows
    ]


def update_elasticsearch():
    es = Elasticsearch([ES_HOST])
    offset = 0

    while True:
        data_chunk = fetch_data_chunk(offset, CHUNK_SIZE)
        if not data_chunk:
            break

        helpers.bulk(es, data_chunk)
        print(f"Pushed {len(data_chunk)} records to Elasticsearch.")
        offset += CHUNK_SIZE

    print("Data sync to Elasticsearch completed.")


if __name__ == "__main__":
    update_elasticsearch()