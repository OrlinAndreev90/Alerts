# Introduction 
Python Functions to obtain failed pipeline runs from azure data factory run history and send out email notifications.


Currently there are 2 functions active ,deployed as AZ Funcs in :  Function app :zzg-dev-reporting-af-001 , Resource group : Reporting_Automation , subscription : Azure CSP


zz-reporting-db-alert:  Queries azure data factory (zz-reporting-db) for failed runs and inserts them in 
[zz-live-reporting-db].[Alerts].[DataFactoryFailedRunsLog]. Runs on a pre-defined schedule : see function.json file

zz-reporting-sendmail : Function to send out email notifications when there have been new failed runs.
Queries  table [zz-live-reporting-db].[Alerts].[DataFactoryFailedRunsLog] , and checks whether there have been any new records inserted after the last time the function has been runned.Runs on a pre-defined schedule : see function.json file


