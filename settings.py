import os
from dotenv import load_dotenv

load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT", 5432),
    "read_host": os.getenv("READ_POSTGRES_HOST")
}
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 50))
INDEX_NAME="analytics_index"
