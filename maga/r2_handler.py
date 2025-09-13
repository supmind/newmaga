import logging
import aioboto3
from botocore.exceptions import ClientError

import config

log = logging.getLogger(__name__)

class R2Handler:
    def __init__(self, endpoint_url, access_key_id, secret_access_key, bucket_name):
        self.bucket_name = bucket_name
        # aioboto3.Session() allows us to create clients and resources that are async.
        self.session = aioboto3.Session(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        self.endpoint_url = endpoint_url
        log.info(f"R2/S3 async client session initialized for endpoint {endpoint_url}")

    async def close(self):
        """
        aioboto3 doesn't have a direct 'close' method on the session.
        Clients are managed via async context managers.
        This method is here for symmetry with other handlers.
        """
        pass

    async def upload_torrent(self, torrent_data: bytes, infohash_hex: str):
        """
        Asynchronously uploads torrent data to the R2 bucket.
        The object key will be the infohash hex string with a .torrent extension.
        """
        object_key = f"{infohash_hex}.torrent"

        try:
            # Use the session to create a client within an async context manager
            async with self.session.client("s3", endpoint_url=self.endpoint_url) as s3_client:
                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=torrent_data,
                    ContentType='application/x-bittorrent'
                )
            log.debug(f"Successfully uploaded {object_key} to R2 bucket {self.bucket_name}")
        except ClientError as e:
            log.error(f"Failed to upload {object_key} to R2: {e}")
            raise
        except Exception as e:
            log.error(f"An unexpected error occurred during R2 upload for {object_key}: {e}")
            raise
