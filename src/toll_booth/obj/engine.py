import boto3


class SnapshotEngine:
    def __init__(self, engine_type, snapshots):
        self._engine_type = engine_type
        self._snapshots = snapshots

    @classmethod
    def build(cls, engine_type, table_name):
        resource = boto3.resource('dynamodb')
        table = resource.Table(table_name)
        identifier = '#'
