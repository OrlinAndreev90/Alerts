from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from . import helper_funcs as hf
import logging , traceback
import pyodbc
import azure.functions as func
import datetime
import os 
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    try:
        results =  hf.get_failed_runs(table="DataFactoryFailedRunsLog",schema="Alerts")
        
        # If there are failed runs  
        if not results.empty:
            datetime_watermark = results['DateAdded'].max()
            period = datetime.datetime.now() - datetime_watermark
            period = period.days* 24 + (period.seconds // 3600)
                ## Send Mail 
            recipients = os.environ['recipients']
            email_sender = os.environ['email_sender']
            hf.send_mail(body=hf.build_table(results,color='blue_light'),period= period, \
                from_= email_sender, to_ = recipients )
            
            hf.update_sent_emails_log(watermark_date=datetime_watermark, to_= recipients)
            logging.info('Python timer trigger function ran at %s. Emails were sent to %s from %s.', utc_timestamp , recipients,email_sender)
        else:
            logging.info('Python timer trigger function ran at %s.No Emails were sent.', utc_timestamp )
    except:
        error_trace = traceback.format_exc()
        logging.error('Error. Info : %s',error_trace)