# maga/config.py

# -- Crawler Settings --
# The default interval in seconds for the crawler to ping bootstrap nodes.
CRAWLER_INTERVAL = 1
# The maximum number of nodes to keep in the processing queue.
NODE_QUEUE_MAXSIZE = 500
# The number of concurrent workers processing nodes from the queue.
NODE_PROCESSOR_CONCURRENCY = 10
# The default port for the DHT crawler to listen on.
DEFAULT_PORT = 6881

# -- Kademlia Settings --
# The size of the K-buckets.
K = 8
# The bootstrap nodes to connect to the DHT network.
BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)

# -- Rate Limiting Settings --
# The time window in seconds for rate limiting.
RATE_LIMIT_WINDOW = 10
# The maximum number of requests allowed per IP address within the time window.
RATE_LIMIT_REQUESTS = 100
# The interval in seconds to clean up stale entries in the rate limiter.
RATE_LIMIT_CLEANUP_INTERVAL = 300

# -- Redis Settings --
# Connection details for Redis server
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Names for the Redis sets used for deduplication
REDIS_PROCESSED_SET = "maga:processed"
REDIS_QUEUED_SET = "maga:queued"


# -- Elasticsearch Settings --
ES_HOST = "localhost"
ES_PORT = 9200
ES_USERNAME = "your_es_username"  # Change this
ES_PASSWORD = "your_es_password"  # Change this
ES_INDEX_NAME = "maga_metadata"


# -- R2/S3 Storage Settings --
# The full endpoint URL for your R2 bucket.
R2_ENDPOINT_URL = "https://<your_account_id>.r2.cloudflarestorage.com"  # Change this
# Your R2 Access Key ID.
R2_ACCESS_KEY_ID = "your_r2_access_key_id"  # Change this
# Your R2 Secret Access Key.
R2_SECRET_ACCESS_KEY = "your_r2_secret_access_key"  # Change this
# The name of the bucket to store metadata files.
R2_BUCKET_NAME = "maga-torrents"  # Change this
