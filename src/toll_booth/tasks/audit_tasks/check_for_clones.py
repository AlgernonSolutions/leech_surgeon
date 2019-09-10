import logging
import os

import difflib
import string
from multiprocessing.pool import ThreadPool

from toll_booth.obj.elastic_driver import ElasticDriver
from toll_booth.obj.inspector import InspectionFinding


def _generate_query(id_source, encounter_id):
    return {
        "bool": {
          "should": [
            {"term": {"encounter_id_value": int(encounter_id)}},
            {"term": {"id_source": id_source}}
          ]
        }
    }


def _retrieve_documentation(driver, id_source, encounter_id):
    logging.debug(f'retrieving documentation for encounter: {encounter_id}')
    query_body = _generate_query(id_source, encounter_id)
    results = driver.search('documentation', {'query': query_body})
    hits = results['hits']['hits']
    if not hits:
        raise RuntimeError(f'could not find encounter with id {encounter_id}')
    if len(hits) > 1:
        raise RuntimeError(f'multiple entries indexed for encounter with id {encounter_id}: {hits}')
    logging.debug(f'received documentation for encounter: {encounter_id}')
    for hit in hits:
        return hit['_source']


def _query_for_clones(driver, source_id, documentation_text):
    query = {
        "bool": {
          "must": [
            {"match": {"documentation_text": {'query': documentation_text}}}
          ],
          "must_not": [
            {"term": {"encounter_id_value": int(source_id)}}
          ]
        }
    }
    logging.debug(f'querying clones for encounter_id: {source_id}')
    results = driver.search('documentation', {'query': query, 'size': 50})
    hits = results['hits']['hits']
    found_values = [(x['_score'], x['_source']) for x in hits]
    logging.debug(f'completed querying for clones for encounter_id: {source_id}')
    return found_values


def _clean_string(dirty_string):
    one_line = dirty_string.replace('\n', '')
    return ''.join(filter(lambda x: x in string.printable, one_line))


def _run_comparison(test_section, clone):
    potential_clone = clone[1]['documentation_text']
    potential_clone = _clean_string(potential_clone)
    try:
        pieces = potential_clone.split('Documentation')[1]
    except IndexError:
        pieces = potential_clone
    diff = difflib.SequenceMatcher(None, test_section, pieces).ratio()
    if diff > 0.8:
        return InspectionFinding(
            'cloning',
            'documentation for this note may be a clone of another note',
            {'potentially_cloned_encounter_id': clone[1]['encounter_id_value']})


def check_for_clones(**kwargs):
    pool = ThreadPool(5)
    logging.info(f'preparing to audit clones for encounter: {kwargs["test_encounter"]}')
    driver = ElasticDriver.generate(os.environ['ELASTIC_HOST'])
    test_encounter = kwargs['test_encounter']
    encounter_id = test_encounter['Service ID']
    id_source = kwargs['id_source']
    results = _retrieve_documentation(driver, id_source, encounter_id)
    documentation_text = results['documentation_text']
    documentation_text = _clean_string(documentation_text)
    note_section = documentation_text.split('Documentation')[1]
    clones = _query_for_clones(driver, encounter_id, note_section)
    clone_findings = pool.starmap(_run_comparison, [(note_section, x) for x in clones])
    filtered_findings = [x for x in clone_findings if x]
    logging.info(f'completed checking for clones for encounter_id: {encounter_id}')
    pool.close()
    pool.join()
    return filtered_findings
