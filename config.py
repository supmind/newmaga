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
