import os
from collections import deque
from queue import Queue
from threading import Thread

from toll_booth.obj.gql.gql_client import GqlClient


def _retrieve_encounter_documentation(work, results):
    gql_endpoint = os.getenv('GRAPH_GQL_ENDPOINT')
    gql_client = GqlClient.from_gql_endpoint(gql_endpoint)
    while True:
        encounter_internal_id = work.get()
        if encounter_internal_id is None:
            return
        documentation = gql_client.get_encounter_documentation(encounter_internal_id)
        filtered_documentation = {x['property_name']: x['property_value']['sensitive'] for x in documentation
                                  if x['property_name'] in ['field_documentation', 'field_name']}
        results.append((encounter_internal_id, filtered_documentation))


def _retrieve_provider_documentation(id_source, provider_id):
    results = deque()
    work = Queue()
    workers = []
    for _ in range(15):
        worker_kwargs = {'work': work, 'results': results}
        worker = Thread(target=_retrieve_encounter_documentation, kwargs=worker_kwargs)
        worker.start()
        workers.append(worker)
    gql_endpoint = os.getenv('GRAPH_GQL_ENDPOINT')
    gql_client = GqlClient.from_gql_endpoint(gql_endpoint)
    encounter_ids, next_token = gql_client.paginate_provider_encounters(id_source, provider_id, 'Community Support')
    for encounter_internal_id in encounter_ids:
        work.put(encounter_internal_id)
    while next_token:
        encounter_ids, next_token = gql_client.paginate_provider_encounters(
            id_source, provider_id, 'Community Support', next_token)
        for encounter_internal_id in encounter_ids:
            work.put(encounter_internal_id)
    for _ in workers:
        work.put(None)
    for worker in workers:
        worker.join()
    return [x for x in results]


def audit_provider(**kwargs):
    id_source = kwargs['id_source']
    bucket_name = kwargs.get('bucket_name', os.getenv('LEECH_BUCKET'))
    provider_id = kwargs['provider_id']
    provider_documentation = _retrieve_provider_documentation(id_source, provider_id)

    return
