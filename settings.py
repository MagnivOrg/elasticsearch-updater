import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
}

REDIS_URL = os.getenv("REDIS_URL")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")

CHUNK_SIZE = 50000
INDEX_NAME = "request_logs_index"
