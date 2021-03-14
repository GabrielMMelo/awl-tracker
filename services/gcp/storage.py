import os
from google.cloud import storage
from dotenv import load_dotenv
from pathlib import Path


class GCPStorage:
    def __init__(self):
        load_dotenv(dotenv_path='../.env')
        self.service_account_path = os.getenv('SERVICE_ACCOUNT_PATH')

    @staticmethod
    def get_credentials_json(self):
        return '{}{}'.format('.secrets/', os.listdir('.secrets')[0])

    def list_blobs_with_prefix(self, bucket_name, prefix, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix."""

        storage_client = storage.Client.from_service_account_json(self.service_account_path)

        blobs = storage_client.list_blobs(
            bucket_name, prefix=prefix, delimiter=delimiter
        )

        return blobs

    def download_blob_as_string(self, bucket_name, source_blob_name):
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"

        storage_client = storage.Client.from_service_account_json(self.service_account_path)

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        bytes = blob.download_as_string()

        return bytes

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client.from_service_account_json(self.service_account_path)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            "[GCS] file {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )

    def upload_from_string(self, bucket_name, content, destination_blob_name, content_type='text/csv'):
        storage_client = storage.Client.from_service_account_json(self.service_account_path)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        return blob.upload_from_string(content, content_type)

    def enable_versioning(self, bucket_name):
        """Enable versioning for this bucket."""
        # bucket_name = "my-bucket"

        storage_client = storage.Client.from_service_account_json(self.service_account_path)

        bucket = storage_client.get_bucket(bucket_name)
        bucket.versioning_enabled = True
        bucket.patch()

        print("Versioning was enabled for bucket {}".format(bucket.name))
        return bucket
