import datetime,logging, traceback
import azure.functions as func
import azure.identity
from . import GetData as helpfuncs
import os 
import requests

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
    try:   
            # Get Failed runs in the last 6 hours
        res= helpfuncs.get_runs_data(df_name = "zz-reporting-db", 
                rg_name= "zigzag-live",period= 6 , subscription_id= os.environ["subscription_id"] )
        # If there are failed runs insert them
        if  res:
            helpfuncs.insert_data_in_db(data=res,table="DataFactoryFailedRunsLog",schema="Alerts")
    
        logging.info('Python timer trigger function ran at %s.\n Failed runs inserted in db : %s ',utc_timestamp, len(res))
    except:
        error_trace = traceback.format_exc()
        logging.error(error_trace)