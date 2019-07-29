import csv
import logging
import os
import re
import uuid
from datetime import datetime
from io import StringIO

import boto3
import rapidjson
from botocore.exceptions import ClientError
from algernon import ajson


def _parse_s3_uri(storage_uri):
    pattern = re.compile(r'(s3://)(?P<bucket>[^/\s]*)(/)(?P<key>[^\s]*)')
    matches = pattern.search(storage_uri)
    bucket_name = matches.group('bucket')
    file_key = matches.group('key')
    return bucket_name, file_key


def retrieve_s3_property(storage_uri: str):
    bucket_name, file_key = _parse_s3_uri(storage_uri)
    stored_property = boto3.resource('s3').Object(bucket_name, file_key)
    response = stored_property.get()
    serialized_property = response['Body'].read()
    object_property = rapidjson.loads(serialized_property)
    return object_property


def build_engine_file_key(operation_type, id_source):
    now = datetime.utcnow().date().isoformat()
    file_key = f'snapshots/{operation_type}/{id_source}/{now}'
    return file_key


def check_for_engine_data(bucket_name, file_key):
    s3 = boto3.resource('s3')
    stored_object = s3.Object(bucket_name, file_key)
    try:
        stored_object.load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise e
        return False


def store_engine_data(bucket_name, file_key, engine_data):
    s3 = boto3.resource('s3')
    stored_object = s3.Object(bucket_name, file_key)
    try:
        stored_object.load()
        raise RuntimeError(f'can not overwrite file at key: {file_key}')
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise e
        stored_object.put(Body=ajson.dumps(engine_data))


def retrieve_stored_engine_data(bucket_name, id_source, cycle_name):
    s3 = boto3.resource('s3')
    file_key = build_engine_file_key(cycle_name, id_source)
    stored_object = s3.Object(bucket_name, file_key)
    response = stored_object.get()
    engine_data = response['Body'].read()
    return rapidjson.loads(engine_data)


def retrieve_stored_data(bucket_name, file_key):
    s3 = boto3.resource('s3')
    stored_object = s3.Object(bucket_name, file_key)
    response = stored_object.get()
    engine_data = response['Body'].read()
    return rapidjson.loads(engine_data)


def store_data_as_csv(bucket_name, file_key, engine_data):
    s3 = boto3.resource('s3')
    stored_object = s3.Object(bucket_name, file_key)
    try:
        stored_object.load()
        raise RuntimeError(f'can not overwrite file at key: {file_key}')
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise e
        write_path = os.path.join('/tmp')
        try:
            save_path = _write_csv_data(engine_data, write_path)
        except FileNotFoundError:
            write_path = os.path.expanduser('~')
            save_path = _write_csv_data(engine_data, write_path)
        stored_object.upload_file(save_path)
        return save_path


def _write_csv_data(engine_data, write_path):
    save_path = str(os.path.join(write_path, uuid.uuid4().hex))
    with open(save_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, doublequote=False, escapechar='\\', quoting=csv.QUOTE_NONNUMERIC)
        for entry in engine_data:
            writer.writerow(entry)
    return save_path


def retrieve_csv_data(bucket_name, file_key, expression):
    results = []
    csv_results = []
    client = boto3.client('s3')
    response = client.select_object_content(
        Bucket=bucket_name,
        Key=file_key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization={'CSV': {
            "FileHeaderInfo": "IGNORE",
            "QuoteEscapeCharacter": "\\",
            "AllowQuotedRecordDelimiter": True
        }},
        OutputSerialization={'CSV': {}},
    )
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            results.append(records)
        elif 'Stats' in event:
            details = event['Stats']['Details']
            logging.debug(f"Stats details bytesScanned: {details['BytesScanned']}")
            logging.debug(f"Stats details bytesProcessed: {details['BytesProcessed']}")
    for result in results:
        csv_string = StringIO(result)
        reader = csv.reader(csv_string)
        for row in reader:
            csv_results.append(row)
    return csv_results
