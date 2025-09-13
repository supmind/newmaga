import logging
from datetime import datetime
from elasticsearch import AsyncElasticsearch

import config

log = logging.getLogger(__name__)

# This is the mapping structure we want.
# Using a Chinese analyzer assumes a plugin like 'analysis-ik' is installed on the ES server.
INDEX_MAPPING = {
    "properties": {
        "infohash": {"type": "keyword"},
        "name": {
            "type": "text",
            "analyzer": "ik_max_word",
            "fields": {
                "raw": {"type": "keyword"}
            }
        },
        "files": {
            "type": "nested",
            "properties": {
                "path": {
                    "type": "text",
                    "analyzer": "ik_max_word",  # Apply Chinese analyzer to file paths as well
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "length": {"type": "long"}
            }
        },
        "total_size": {"type": "long"},
        "discovered_at": {"type": "date"}
    }
}

class ESHandler:
    def __init__(self, hosts, username, password):
        self.client = AsyncElasticsearch(
            hosts,
            http_auth=(username, password)
        )
        log.info(f"Elasticsearch async client initialized for hosts: {hosts}")

    async def close(self):
        """Closes the Elasticsearch client connection."""
        await self.client.close()

    async def init_index(self):
        """
        Initializes the index in Elasticsearch if it doesn't exist.
        """
        try:
            if not await self.client.indices.exists(index=config.ES_INDEX_NAME):
                log.info(f"Creating index '{config.ES_INDEX_NAME}' in Elasticsearch...")
                await self.client.indices.create(
                    index=config.ES_INDEX_NAME,
                    body={"mappings": INDEX_MAPPING}
                )
                log.info(f"Index '{config.ES_INDEX_NAME}' created successfully.")
            else:
                log.info(f"Index '{config.ES_INDEX_NAME}' already exists.")
        except Exception as e:
            log.error(f"Failed to initialize Elasticsearch index: {e}")
            raise

    async def upload_document(self, infohash, name, files_list, total_size):
        """
        Asynchronously creates and saves a torrent document in Elasticsearch.
        """
        doc_source = {
            "infohash": infohash,
            "name": name,
            "total_size": total_size,
            "discovered_at": datetime.now()
        }
        if files_list:
            # Decode file paths from bytes to string, handling potential errors
            doc_source["files"] = [
                {
                    "path": "/".join(p.decode('utf-8', 'ignore') for p in f.get(b'path', [])),
                    "length": f.get(b'length', 0)
                }
                for f in files_list
            ]

        try:
            await self.client.index(
                index=config.ES_INDEX_NAME,
                id=infohash,
                document=doc_source
            )
            log.debug(f"Successfully uploaded document to ES: {infohash}")
        except Exception as e:
            log.error(f"Failed to upload document to ES for infohash {infohash}: {e}")
            raise
