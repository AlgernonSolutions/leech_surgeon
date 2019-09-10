import os

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search, Q
from requests_aws4auth import AWS4Auth
import boto3


class ElasticDriver:
    def __init__(self, es_host, aws_auth):
        self._es_host = es_host
        self._aws_auth = aws_auth

    @classmethod
    def generate(cls, es_host):
        credentials = boto3.Session().get_credentials()
        region = os.environ.get('AWS_REGION', 'us-east-1')
        if os.getenv('AWS_SESSION_TOKEN', None):
            session_token = os.environ['AWS_SESSION_TOKEN']
            auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=session_token)
            return cls(es_host, auth)
        aws_auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es')
        return cls(es_host, aws_auth)

    @property
    def es_client(self):
        return Elasticsearch(
            hosts=[{'host': self._es_host, 'port': 443}],
            http_auth=self._aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    def index_document(self, index_name, document_type, document_id, document):
        return self.es_client.create(index=index_name, doc_type=document_type, id=document_id, body=document)

    def get_document(self, index_name, document_type, document_id):
        return self.es_client.get(index=index_name, doc_type=document_type, id=document_id)

    def search(self, index_name, query_body):
        # search = Search(using=self.es_client).index(index_name).query(query_body)
        try:
            return self.es_client.search(index=index_name, body=query_body, request_timeout=45)
        except Exception as e:
            print(e)
            raise e

    def find_clones(self, encounter_id, documentation_text):
        search = Search(using=self.es_client).index('documentation')
        query = Q(
            'bool',
            must=[Q('match', documentation_text=documentation_text)],
            must_not=[Q('term', encounter_id=encounter_id)]
        )
        search = search.query(query)
        test = query.to_dict()
        return search.execute()

    def get_max_id_value(self, id_source, object_type):
        body = {
            "aggs": {
                "max_value": {
                    "filter": {"term": {"id_source": id_source}},
                    "aggs": {"top_value": {"max": {"field": "id_value"}}}
                }
            },
            "size": 0
        }
        index_name = object_type.lower()
        response = self.es_client.search(index=index_name, body=body)
        return response['aggregations']['max_value']['top_value']['value']
