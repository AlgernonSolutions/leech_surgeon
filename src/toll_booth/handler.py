from algernon.aws import lambda_logged
import toll_booth


@lambda_logged
def handler(event, context):
    toll_booth.engine_handler(event, context)
