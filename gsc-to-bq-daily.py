#Google Search Console to BigQuery Data Pull - Daily - For Google Cloud Functions
#Written by Kevin Germain for Zenith Global

from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

########### SET YOUR PARAMETERS HERE ####################################
PROPERTIES = ["https://www.example.com/"]
BQ_DATASET_NAME = 'DatasetName' #Name of the BigQuery dataset that contains your tables, needs to exist prior to the script run
BQ_TABLE_NAME = 'Table_Name' #Name of the BigQuery table that will receive your data, it does not need to exist prior to the script run
SERVICE_ACCOUNT_FILE = 'serviceAccount.json' #Name of the service account file
N_DAYS_AGO = 5 #GSC only allows to pull the data 3 days before current date, 5 here for safety
MAX_ROWS = 15000000 #Max number of rows to pull, if you want all data, input a high number
################ END OF PARAMETERS ######################################

START_DATE = str(datetime.now().date() - timedelta(days=N_DAYS_AGO))
END_DATE = START_DATE

SCOPES = ['https://www.googleapis.com/auth/webmasters']
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('searchconsole', 'v1', credentials=credentials)

def getSearchConsoleData(site_url, start_date, end_date, start_row):

    request = {
      'startDate': start_date,
      'endDate': end_date,
      'dimensions': ['query','device', 'page', 'date'],
      'rowLimit': 25000,
      'startRow': start_row
       }

    response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()

    if len(response) > 1:

        x = response['rows']

        df = pd.DataFrame.from_dict(x)
        
        # split the keys list into columns
        df[['query','device', 'page', 'date']] = pd.DataFrame(df['keys'].values.tolist(), index= df.index)
        
        # Drop the key columns
        df = df.drop(['keys'],axis=1)

        # Add a website identifier
        df['website'] = site_url

        return df
    else:
        print("There are no more results to return.")
    
def loadToBigQuery(df):
    # establish a BigQuery client
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

    dataset_id = BQ_DATASET_NAME
    table_name = BQ_TABLE_NAME
    
    # create a job config
    job_config = bigquery.LoadJobConfig()
    
    # Set the destination table
    table_ref = client.dataset(dataset_id).table(table_name)
    #job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_APPEND'

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()
    
def main(self):
    # Loop through all defined properties, for up to n rows of data in each
    for p in PROPERTIES:
        for x in range(0,1000000,25000):
            y = getSearchConsoleData(p, START_DATE, END_DATE, x)
            loadToBigQuery(y)
            print(x, "done")
            if len(y) < 25000:
                break
            else:
                continue
    return 'OK'
