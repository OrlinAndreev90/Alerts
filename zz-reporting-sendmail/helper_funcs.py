""" Module with functions to check for failed runs in zz-live-reporting-db.Alerts.DataFactoryFailedRunsLog and send emails"""

from sqlalchemy import sql,func
from sqlalchemy.ext.declarative import declarative_base
from pretty_html_table import build_table
from datetime import datetime
from smtplib import SMTP , SMTP_SSL
import sqlalchemy
import pandas as pd
import os 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import re 
def get_failed_runs( table :str,schema:str) ->pd.DataFrame:
    """ Function to extract data about failed runs from zz-live-reporting-db.Alerts.DataFactoryFailedRunsLog
 that have have been added to the system.
    parameters:
        table: String containing the name of the table to be queried for failed runs 
        schema: schema of the table
    Logic:  First query the watermarkstable(Alerts.SentMails) for the latest watermark
        in order to get datetime_watermark.
        Then get query runs according to (Include: DateAdded > datetime_watermark). 
     Assumption . Table that contains watermarks is in the same schema as the table with the failed runs.
                    Name of the watermark table : SentEmails
    Output: Pandas DataFrame containing the Results
    """  
    username_dest =  os.environ["usernameDest"]
    pwd_dest =  os.environ["pwdDest"]
    dsn_dest =  os.environ["dsnDest"]
    engine_dest = sqlalchemy.create_engine("mssql+pyodbc://{0}:{1}@{2}" \
        .format(username_dest,pwd_dest,dsn_dest),isolation_level='READ COMMITTED', echo = False)

    ## Copy table schema from the db
    meta_dest = sqlalchemy.MetaData(schema= schema)
    base = declarative_base()
    
    class DataFactoryFailedRunsLog(base):
        __table__ = sqlalchemy.Table(table,meta_dest,autoload=True, autoload_with= engine_dest )
    
     # Table which contains the the datetime up to which the failed runs data was checked.
    sent_mails_table = sqlalchemy.Table("SentMails",meta_dest,autoload=True, autoload_with= engine_dest ) 
     

    datetime_watermark_query = sql.select([func.max(sent_mails_table.c.WatermarkDate)]).select_from(sent_mails_table)
    datetime_watermark = engine_dest.execute(datetime_watermark_query).fetchone()[0].strftime('%Y-%m-%d %H:%M%S.%f')[:-3]
        #In case there are no  watermarks in the Watermarks table
    if datetime_watermark is None:
        # Dont Filter on watermark
        query= sql.select([DataFactoryFailedRunsLog])
    else:
        query= sql.select([DataFactoryFailedRunsLog]).where(DataFactoryFailedRunsLog.DateAdded > datetime_watermark)
    
       # Store QUery Results in pandas dataframe
    results = pd.read_sql_query(query,con=engine_dest.connect())

    return results


def send_mail(body ,period :int,from_ :str ,to_ :str):
    """ Function To Send Mail from a gmail account """
    message = MIMEMultipart()
    message['Subject'] = 'Failed Pipeline Runs in the last  %s hours' %(period)
    message['From'] =  from_ #'automation@zigzag.global'
    message['To'] = to_

    body_content = body
    message.attach(MIMEText(body_content, "html"))
    msg_body = message.as_string()
    server = SMTP('smtp.gmail.com', 587)
    
    server.starttls()
    server.login(message['From'], '%9mtKRpqsm6c&RHQ4eB$')
    server.sendmail(message['From'],  re.split(",|;",message['To']), msg_body)
    server.quit()





def update_sent_emails_log(watermark_date :datetime,to_ :str):
    """Function to update table Alerts.SentMails when an email has been sent. 
    Using pre-defined connection credentials. 
    parameters:
    watermark_date: last processed failed run
    to_: a string with the e-mail addresses it was sent to.

    """
    username_dest =  os.environ["usernameDest"]
    pwd_dest =  os.environ["pwdDest"]
    dsn_dest =  os.environ["dsnDest"]
    engine_dest = sqlalchemy.create_engine("mssql+pyodbc://{0}:{1}@{2}" \
        .format(username_dest,pwd_dest,dsn_dest),isolation_level='READ COMMITTED', echo = False)

    
    meta_dest = sqlalchemy.MetaData(schema= "Alerts")
    sent_mails_table = sqlalchemy.Table("SentMails",meta_dest,autoload=True, autoload_with= engine_dest )

    # Construct Insert Statement
    insert_expression = sqlalchemy.sql.insert(sent_mails_table, values={"DateSent":datetime.now(),
    "WatermarkDate": watermark_date,"To":to_}) 

    engine_dest.execute(insert_expression)
