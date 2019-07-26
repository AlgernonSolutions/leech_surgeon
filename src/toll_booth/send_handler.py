import logging
import os

from algernon.aws import lambda_logged
import boto3


def _send_by_ses(recipients, subject_line, html_body, text_body):
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
    return table.get_item(Key={'identifier': identifier, 'sid_value': id_source})


@lambda_logged
def send_handler(event, context):
    logging.info(f'received a call to run send_report: {event}/{context}')
    subject_line = 'Algernon Solutions Clinical Intelligence Report'
    download_link = event['download_link']
    id_source = event['id_source']
    table_name = os.environ['CLIENT_DATA_TABLE']
    recipients = _retrieve_report_recipients(id_source, table_name)
    text_body = _generate_text_body(download_link)
    html_body = _generate_html_body(download_link)
    response = _send_by_ses(recipients['recipients'],subject_line, html_body, text_body)
    results = {'message_id': response['MessageId'], 'text_body': text_body, 'html_body': html_body}
    logging.info(f'completed a call to run send_report: {event}/{results}')
    return results
