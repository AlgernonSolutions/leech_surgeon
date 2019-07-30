import logging
import os

from algernon import rebuild_event
from algernon.aws import lambda_logged
import boto3

from toll_booth.obj.reports import ReportData
from toll_booth.tasks.s3_tasks import retrieve_stored_engine_data
from toll_booth.tasks.write_report_data import write_report_data


def _send_by_ses(recipient, subject_line, html_body, text_body):
    client = boto3.client('ses')
    response = client.send_email(
        Source='algernon@algernon.solutions',
        Destination={
            'ToAddresses': [recipient['email_address']]
        },
        Message={
            'Subject': {'Data': subject_line},
            'Body': {
                'Text': {'Data': text_body},
                'Html': {'Data': html_body}
            }
        },
        ReplyToAddresses=['algernon@algernon.solutions']
    )
    return response


def _batch_send_by_ses(recipients, subject_line, html_body, text_body):
    client = boto3.client('ses')
    response = client.send_email(
        Source='algernon@algernon.solutions',
        Destination={
            'ToAddresses': [x['email_address'] for x in recipients]
        },
        Message={
            'Subject': {'Data': subject_line},
            'Body': {
                'Text': {'Data': text_body},
                'Html': {'Data': html_body}
            }
        },
        ReplyToAddresses=['algernon@algernon.solutions']
    )
    return response


def _generate_text_body(download_link):
    return f''' 
            You are receiving this email because you have requested to have routine reports sent to you through the 
            Algernon Clinical Intelligence Platform. 
            The requested report can be downloaded from the included link. To secure the information contained within, 
            the link will expire in {download_link.expiration_hours} hours. 
            I hope this report brings you joy and the everlasting delights of a cold data set.

            {str(download_link)}

            - Algernon

            This communication, download link, and any attachment may contain information, which is sensitive, 
            confidential and/or privileged, covered under HIPAA and is intended for use only by the addressee(s) 
            indicated above. If you are not the intended recipient, please be advised that any disclosure, copying, 
            distribution, or use of the contents of this information is strictly prohibited. If you have received this 
            communication in error, please notify the sender immediately and destroy all copies of the original 
            transmission. 
        '''


def _generate_html_body(download_link):
    return f'''
          <!DOCTYPE html>
           <html lang="en" xmlns="http://www.w3.org/1999/html" xmlns="http://www.w3.org/1999/html">
               <head>
                   <meta charset="UTF-8">
                   <title>Algernon Clinical Intelligence Report</title>
                   <style>

                       .container {{
                           position: relative;
                           text-align: center;
                       }}
                   </style>
               </head>
               <body>
                   <p>You are receiving this email because you have requested to have routine reports sent to you 
                   through the Algernon Clinical Intelligence Platform.</p> 
                   <p>The requested report can be downloaded from the included link. To secure the information contained 
                   within, the link will expire in {download_link.expiration_hours} hours.</p> 
                   <p>I hope this report brings you joy and the everlasting delights of a cold data set.</p>
                   <h4><a href="{str(download_link)}">Download Report</a></h4>
                   <p> - Algernon </p>
                   <h5>
                       This communication, download link, and any attachment may contain information, which is 
                       sensitive, confidential and/or privileged, covered under HIPAA and is intended for use only by 
                       the addressee(s) indicated above.<br/> 
                       If you are not the intended recipient, please be advised that any disclosure, copying, 
                       distribution, or use of the contents of this information is strictly prohibited.<br/> 
                       If you have received this communication in error, please notify the sender immediately and 
                       destroy all copies of the original transmission.<br/> 
                   </h5>
               </body>
           </html>
       '''


def _retrieve_report_recipients(id_source, table_name):
    resource = boto3.resource('dynamodb')
    table = resource.Table(table_name)
    identifier = '#report_recipients#'
    response = table.get_item(Key={'identifier_stem': identifier, 'sid_value': id_source})
    return [x for x in response['Item']['recipients']]


def _generate_base_report(id_source, bucket_name):
    report_data = retrieve_stored_engine_data(bucket_name, id_source, 'built_reports')
    report_data = rebuild_event(report_data)
    return {x: ReportData.from_stored_data(x, y) for x, y in report_data.items()}


def _supervisor_filter(row_entry, supervisor_last_name):
    return row_entry['team_name'] == supervisor_last_name


def _csw_filter(row_entry, csw_last_name, csw_first_name):
    return row_entry['csw_name'] == f'{csw_last_name}, {csw_first_name}'


def _generate_individual_report(recipient, base_report):
    non_filtered = ['unassigned']
    report_role = recipient['role']
    last_name = recipient['last_name']
    first_name = recipient['first_name']
    report_data = {}
    for entry_name, report_entry in base_report.items():
        if entry_name in non_filtered:
            if report_role != 'all':
                continue
            report_data[entry_name] = report_entry
            continue
        if report_role == 'supervisor':
            report_data[entry_name] = report_entry.line_item_filter(_supervisor_filter, (last_name,))
            continue
        if report_role == 'csw':
            report_data[entry_name] = report_entry.line_item_filter(_csw_filter, (last_name, first_name))
            continue
        report_data[entry_name] = report_entry
    return report_data


@lambda_logged
def send_handler(event, context):
    results = []
    logging.info(f'received a call to run send_report: {event}/{context}')
    subject_line = 'Algernon Solutions Clinical Intelligence Report'
    id_source = event['id_source']
    table_name = os.environ['CLIENT_DATA_TABLE_NAME']
    bucket_name = os.environ['LEECH_BUCKET']
    base_report = _generate_base_report(id_source, bucket_name)
    recipients = _retrieve_report_recipients(id_source, table_name)
    for recipient in recipients:
        recipient_report = _generate_individual_report(recipient, base_report)
        report_name = f'daily_report_{recipient["last_name"]}_{recipient["first_name"]}'
        download_link = write_report_data(
            report_name=report_name, id_source=id_source, report_data=recipient_report)
        text_body = _generate_text_body(download_link)
        html_body = _generate_html_body(download_link)
        response = _send_by_ses(recipient, subject_line, html_body, text_body)
        result = {'message_id': response['MessageId'], 'download_link': download_link, 'recipient': recipient}
        results.append(result)
    logging.info(f'completed a call to run send_report: {event}/{results}')
    return results
