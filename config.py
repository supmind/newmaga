import os

# -- Elasticsearch Configuration --
# The connection URL for your Elasticsearch cluster.
# Example: "http://localhost:9200"
# Can be overridden by environment variable: ES_HOSTS
ES_HOSTS = os.environ.get("ES_HOSTS", "http://localhost:9200")

# The name of the Elasticsearch index where metadata will be stored.
# Can be overridden by environment variable: ES_INDEX
ES_INDEX = os.environ.get("ES_INDEX", "torrent_metadata")

# -- Optional: Elasticsearch Authentication --
# Username for Elasticsearch authentication.
# Can be overridden by environment variable: ES_USERNAME
ES_USERNAME = os.environ.get("ES_USERNAME")

# Password for Elasticsearch authentication.
# Can be overridden by environment variable: ES_PASSWORD
ES_PASSWORD = os.environ.get("ES_PASSWORD")


# -- R2 (S3-Compatible) Storage Configuration --
# Your R2 account ID.
# Can be overridden by environment variable: R2_ACCOUNT_ID
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")

# The Access Key ID for your R2 bucket.
# Can be overridden by environment variable: R2_ACCESS_KEY_ID
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")

# The Secret Access Key for your R2 bucket.
# Can be overridden by environment variable: R2_SECRET_ACCESS_KEY
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")

# The name of the R2 bucket where .torrent files will be stored.
# Can be overridden by environment variable: R2_BUCKET_NAME
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")

# The endpoint URL for the R2 service.
# This is constructed using your account ID.
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None


# -- Screenshot Service Configuration --
# The URL for the screenshot submission service.
# Can be overridden by environment variable: SCREENSHOT_URL
SCREENSHOT_URL = os.environ.get("SCREENSHOT_URL", "http://47.79.230.210:8000/tasks/")

# The API key for the screenshot submission service.
# Can beridden by environment variable: SCREENSHOT_API_KEY
SCREENSHOT_API_KEY = os.environ.get("SCREENSHOT_API_KEY", "a_very_secret_and_complex_key_for_dev")


# -- Crawler Configuration --
# Helper function to get integer values from environment variables, with a default.
def _get_int_env(key, default):
    value = os.environ.get(key)
    if value and value.isdigit():
        return int(value)
    return default

# Port to listen on for DHT traffic.
# Can be overridden by environment variable: CRAWLER_PORT
CRAWLER_PORT = _get_int_env("CRAWLER_PORT", 6881)

# Number of concurrent metadata download workers.
# Can be overridden by environment variable: CRAWLER_WORKERS
CRAWLER_WORKERS = _get_int_env("CRAWLER_WORKERS", 200)

# Maximum size of the task queue.
# Can be overridden by environment variable: CRAWLER_QUEUE_SIZE
CRAWLER_QUEUE_SIZE = _get_int_env("CRAWLER_QUEUE_SIZE", 2000)
