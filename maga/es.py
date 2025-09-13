from datetime import datetime
from elasticsearch_dsl import Document, Text, Keyword, Long, Nested, Date, InnerDoc
from elasticsearch_dsl.analysis import analyzer

# Note on Chinese Analyzer:
# This Document definition uses the 'ik_smart' tokenizer, which is effective for
# Chinese language text. For this to work, the 'elasticsearch-analysis-ik' plugin
# must be installed on your Elasticsearch cluster.
#
# You can find installation instructions here:
# https://github.com/medcl/elasticsearch-analysis-ik
#
# We are defining a custom default analyzer for the index that uses this tokenizer.
chinese_analyzer = analyzer(
    'chinese_analyzer',
    tokenizer='ik_smart'
)

class FileInfo(InnerDoc):
    """
    Represents a single file within a torrent. This will be stored as a nested
    document within the main TorrentMetadata document.
    """
    path = Text(fields={'raw': Keyword()}) # Store path for full-text search and as a raw keyword
    length = Long()

class TorrentMetadata(Document):
    """
    Represents the metadata of a torrent, structured for storage and searching
    in Elasticsearch.
    """
    info_hash = Keyword()
    # The 'name' field will be analyzed for full-text search, including Chinese.
    name = Text(analyzer='ik_smart', fields={'raw': Keyword()})
    files = Nested(FileInfo)
    total_size = Long()
    discovered_date = Date()

    class Index:
        # The index name will be dynamically set from the config file.
        # This 'name' is a placeholder.
        name = 'torrent_metadata'
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            # Set the default analyzer for the index to our Chinese analyzer.
            # This is not strictly necessary as we specify it on the field,
            # but it is good practice.
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "ik_smart"
                    }
                }
            }
        }

    async def save(self, **kwargs):
        """
        Overrides the default save method to automatically set the
        'discovered_date' before saving the document.
        """
        # Ensure the index name from the config is used.
        if 'index' not in kwargs:
            from config import ES_INDEX
            kwargs['index'] = ES_INDEX

        self.discovered_date = datetime.now()
        # The 'elasticsearch-dsl' library's async save method is what we're calling here.
        return await super().save(**kwargs)

    @classmethod
    async def init(cls, using=None):
        """
        Initializes the index in Elasticsearch, creating it with the defined
        mappings and settings. It's idempotent and won't fail if the index
        already exists.
        """
        from config import ES_INDEX
        # The 'init' method is part of the 'Index' class in elasticsearch-dsl.
        # We call it on the document's _index attribute.
        # It requires the index name to be set.
        cls._index._name = ES_INDEX
        await cls._index.init(using=using)
