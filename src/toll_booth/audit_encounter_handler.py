import logging
import uuid

from algernon import rebuild_event, ajson
from algernon.aws import lambda_logged, StoredData

from toll_booth.tasks.audit_tasks import audit_encounter


@lambda_logged
def audit_encounter_handler(event, context):
    event = rebuild_event(event)
    logging.info(f'started to audit an encounter: {event}/{context}')
    audit_results = audit_encounter(**event)
    stored_results = StoredData.from_object(uuid.uuid4(), audit_results, True)
    return ajson.dumps(stored_results)
