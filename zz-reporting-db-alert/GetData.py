""" 
"""

from  azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient 
from azure.mgmt.datafactory.models import RunFilterParameters,RunQueryFilter, \
    RunQueryFilterOperand , RunQueryFilterOperator
from datetime import datetime, timedelta
#import time
#import pandas as pd
#import collections
import sqlalchemy
from sqlalchemy.orm import (scoped_session, sessionmaker)
from sqlalchemy.ext.declarative import declarative_base
import os


def get_runs_data(df_name: str, rg_name :str, period :int, subscription_id :str ) -> list :
    
    """
    Function to retrieve failed runs data from Azure Data Factory (df_name) in resource group(rg_name)
    in a  subscription (subscription_id) for a chosen amount of days(period)

    parameters:
        df_name: Data factory name
        rg_name: Resource group name
        period: Number of hours back to capture , from the time of execution.
        subscription_id : Subscription Id of the data factory
    """

    #subscription_id= os.environ["subscription_id"] "e54046bb-9b95-4180-ba13-4d76bf54f5f8"  # use os.environ
    credentials =  DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credentials, subscription_id)



    ## Set Query filter Parameters : All failed pipeline runs in last ( period)
    filter_params = RunFilterParameters(
        last_updated_after= datetime.now() - timedelta(hours = period), 
        last_updated_before=datetime.now() + timedelta(hours = period),
        filters= [RunQueryFilter(  operand =RunQueryFilterOperand('Status'), 
        operator = RunQueryFilterOperator('Equals') , values=['Failed'])] )


    # Run The Query 
    query_response = adf_client.pipeline_runs.query_by_factory( resource_group_name= rg_name ,
                                                        factory_name= df_name, filter_parameters = filter_params)   
    
    

    
    results = list()
   # Extract relevant fields
    for runs in query_response.value:                                                   
        datarecord= {"PipelineRunId" :runs.run_id, "PipelineName":runs.pipeline_name, \
           "RunStart": runs.run_start, "ErrorMessage": runs.message,
           "DataFactoryName": df_name,"ResourceGroup": rg_name }
        results.append(datarecord)
    return (results)



def insert_data_in_db(data : list , table :str,schema:str) -> None:
    """
        Function to insert pandas dataframe in a a SQL db .  using pre-defined connection information.
        Paramaters:
            data : Pandas dataframe that contains the data to be loaded.
            table : name of the table in the database
            schema : name of the schema of the table    
    """


    username_dest =  os.environ["usernameDest"]
    pwd_dest =  os.environ["pwdDest"]
    dsn_dest =  os.environ["dsnDest"]

    engine_dest = sqlalchemy.create_engine("mssql+pyodbc://{0}:{1}@{2}".format(username_dest,pwd_dest,dsn_dest),\
                                    isolation_level='READ COMMITTED', echo = False)

    ## Copy table schema from the db
    meta_dest = sqlalchemy.MetaData(schema= schema)
    base = declarative_base()
    class DataFactoryFailedRunsLog(base):
        __table__ = sqlalchemy.Table(table,meta_dest,
                            sqlalchemy.Column('DateAdded',  sqlalchemy.DATETIME, default=datetime.now),   
                                autoload=True, autoload_with= engine_dest )
    
    
    
    # Start a db session
    db_session = scoped_session(sessionmaker(bind=engine_dest))
    
    # Add the Rows : Insert new records and update old ones if they overlap
    for record in data:
        row = DataFactoryFailedRunsLog(**record)
        db_session.merge(row)

    db_session.commit()
    db_session.close()


    