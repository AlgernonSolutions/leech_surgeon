import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from algernon import AlgObject


class ObjectDownloadLink(AlgObject):
    def __init__(self, bucket_name, remote_file_path, expiration_seconds=172800, local_file_path=None):
        self._bucket_name = bucket_name
        self._remote_file_path = remote_file_path
        self._expiration_seconds = expiration_seconds
        self._local_file_path = local_file_path
        self._stored = False

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['bucket_name'], json_dict['remote_file_path'],
            json_dict.get('expiration_seconds', 172800), json_dict.get('local_file_path')
        )

    def _store(self):
        s3 = boto3.resource('s3')
        if self._local_file_path is None:
            raise RuntimeError(f'tried to generate a download url for {self._remote_file_path}, '
                               f'but it does not exist remotely, and no local path was provided')
        s3.Bucket(self._bucket_name).upload_file(self._local_file_path, self._remote_file_path)
        self._stored = True

    def _check(self):
        if self._stored is True:
            return True
        resource = boto3.resource('s3')
        object_resource = resource.Object(self._bucket_name, self._remote_file_path)
        try:
            object_resource.load()
        except ClientError as e:
            return int(e.response['Error']['Code']) != 404
        return True

    def __str__(self):
        client = boto3.client('s3', config=Config(signature_version='s3v4'))
        if not self._stored:
            self._store()
        pre_signed = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self._bucket_name,
                'Key': self._remote_file_path
            },
            ExpiresIn=self._expiration_seconds
        )
        return pre_signed

    @property
    def expiration_hours(self):
        return int(self._expiration_seconds/3600)
