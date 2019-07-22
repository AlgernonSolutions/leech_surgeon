import logging


def task(task_name):
    def task_wrapper(production_fn):
        def wrapper(**kwargs):
            logging.info(f'starting task: {task_name}, kwargs: {kwargs}')
            results = production_fn(**kwargs)
            logging.info(f'completed task: {task_name}, results: {results}')
            return results
        return wrapper
    return task_wrapper
