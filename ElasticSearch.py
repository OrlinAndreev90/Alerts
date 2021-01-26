
import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import HelperFuncs as hf
import pandas  as pd
import datetime
from dateutil.relativedelta import relativedelta
format='%Y-%m-%d %H:%M:%S'
def QueryData(QueryDate,format,source_index):

    QueryDatestr = datetime.datetime.strftime(QueryDate,format=format)
    QueryDateLowerBoundstr = datetime.datetime.strftime(QueryDate - datetime.timedelta(hours=12),format=format)
    ## Connect to the Elastic Search db
    es=  Elasticsearch([     'https://user:secret@cb5b186b27c5428882fc9787a686385e.europe-west1.gcp.cloud.es.io:9243'])

    ## the data is kept in index : zz-carriers-api-prod-v1
    # _source_includes: A list of fields to extract and return
    #         from the _source field
    ## Compose Seach query using Elasticsearch DSL . Filter on the @timestamp column
    query =\
        { "query":
            {"bool":
                {
                  "filter":
                       [
                           {"range":
                                {"@timestamp": { "lte": QueryDatestr,
                                                 "gt": QueryDateLowerBoundstr,
                                                 "format":'yyyy-MM-dd HH:mm:ss'} }
                            },

                            {
                                "match_phrase":
                                    {"message":
                                         {"query": "414d27b4-771c-42ff-81ae-cbe7d4365617"}
                                     }
                            }
                       ],
                  "must_not":
                    [
                        { "match":
                          { "message": "InternalErrorResponse"}  # Exclude Error Messages
                      },
                       {
                        "match_phrase":
                          {"message": { "query" : "Internal error"}
                           }
                       }

                     ]



                   }
             }
          }

    # Extract the Data
    ## Compose a function to Load the data using the Search and Scroll Api

     # Reutnrs an iterator holding the serach results
    Test = helpers.scan(es,query=query,scroll='2m',size=500,index= source_index,_source_includes=['@timestamp','message'],timeout='120s')

    Result =[]

    for document in Test:
        Result.append( [document['_id'],document['_source']['@timestamp'], document['_source']['message']])
    return Result


## Extract Data on a Daily Basis
###Create A date Range for which  the data will be extractedW
DownloadedData_ = []
#base = datetime.datetime.today()
base = datetime.datetime(2021, 1, 1, 0, 0, 0, 0) ## User Input
intervals =  (base -   (base + relativedelta(months=-1))).days  *2

date_list2 = [base - datetime.timedelta(hours= x) for x in range(0,intervals * 12,12)]
##'zz-carriers-api-prod-v1'
##'zz-carriers-api-dev-v1'
DownloadedDataTemp = [QueryData(QueryDate= date,format='%Y-%m-%d %H:%M:%S',source_index= 'zz-carriers-api-prod-v1') for date in date_list2]

DownloadedData_+=  DownloadedDataTemp


## Export Results to csv before processing in case of Error
#Export =pd.DataFrame(DownloadedData_)
#Export.to_csv('DownloadedData_20200728_20200331.csv',sep=';',index=False,header=True,chunksize=10000)




## Parse |Data
Result =  [ pd.DataFrame( hf.ParseResultData(InputData=day) ,columns=['Id','timestamp','Data']) for day in DownloadedData_]

# Convert The inner Elements into a data frame
#TestData = [pd.DataFrame(result,columns=['Id','timestamp','Data']) for result in TestResult ]
Data = pd.concat(Result)
Data.reset_index(inplace=True)

Data[['ExternalOrderId','TrackingId','ReturnOrderUniqueIdentifier','ReturnCurrency','CountryName','RetailerName','CarrierName']] = \
    pd.json_normalize(Data['Data'])

Data.drop('Data',axis=1,inplace=True)
Data.drop('index',axis=1,inplace=True)
#Data.to_csv('ProcessedData_20200731_20200728.csv',sep=';',index=False,header=True,chunksize=10000)


## Read

FinalData = Data # pd.concat([Data1,Data2,Data])

FinalData.to_csv('CombinedData_20200731_2019.csv',sep=';',index=False,header=True,chunksize=10000)


## Load Data in db.
## Option 1 : use sql alchemy to INSERT the data on a row by row basis. ( slower option)
import sqlalchemy
# Create Table

usernameDest = 'username'
pwdDest = 'password'
dsnDest = 'connectionstring'

engineDest = sqlalchemy.create_engine("mssql+pyodbc://{0}:{1}@{2}".format(usernameDest,pwdDest,dsnDest),\
                                  isolation_level='READ COMMITTED', echo = True)


Meta = sqlalchemy.MetaData()


PentlandData= sqlalchemy.Table('PentlandData',Meta,
                               sqlalchemy.Column('RecordId',sqlalchemy.types.Integer,autoincrement=True),
                               sqlalchemy.Column('ElasticSearchDbId',sqlalchemy.types.NVARCHAR(50)),
                               sqlalchemy.Column('TimeStamp',sqlalchemy.types.DATETIME),

                               sqlalchemy.Column('ExternalOrderId', sqlalchemy.types.NVARCHAR(200)),
                               sqlalchemy.Column('TrackingId', sqlalchemy.types.NVARCHAR(200)),
                               sqlalchemy.Column('ReturnOrderUniqueIdentifier', sqlalchemy.types.NVARCHAR(200), ),
                               sqlalchemy.Column('ReturnCurrency', sqlalchemy.types.NVARCHAR(50)),
                               sqlalchemy.Column('CountryName', sqlalchemy.types.NVARCHAR(50)),
                               sqlalchemy.Column('RetailerName', sqlalchemy.types.NVARCHAR(50)),
                               sqlalchemy.Column('CarrierName', sqlalchemy.types.NVARCHAR(50))
                               )




FinalData.rename(columns={"Id":"ElasticSearchDbId","timestamp":"TimeStamp"},inplace=True)
FinalData['TimeStamp']= pd.to_datetime(FinalData['TimeStamp'],format ='%Y-%m-%dT%H:%M:%S.%f%z')
FinalData.to_sql('PentlandData',con=engineDest,if_exists='append',index=False)


