import pandas as pd
from google.cloud import storage
import os
import io
from datetime import datetime, timedelta
from pandas_gbq import to_gbq, read_gbq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='YOUR-GCP-CREDS.JSON'

try:
    now = datetime.now() # current date and time
    date_time = now- timedelta(days=1)
    filename = 'FILE-NAME'+datetime.strftime(date_time , "%d%m%Y")+'.csv'
    print("date and time:",filename)
    
    bucket_name='YOUR-BUCKET-NAME'
    source_file_name = filename
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    data = blob.download_as_string()
    df = pd.read_csv(io.BytesIO(data),sep='|', error_bad_lines=False)
    print(f'Pulled down file from bucket {bucket_name}, file name: {source_file_name}')
    
    df.columns=df.columns.str.replace('[#,@,&,-,=,+, ,:,.,/,%]','')
    df['SaleDate']= pd.to_datetime(df['SaleDate'], format = '%d/%m/%Y %H:%M:%S')
    df['SaleDate'] = df['SaleDate'].dt.date    
    
    to_gbq(df, 'YOUR-DATASET.YOUR-TABLE-NAME', project_id='YOUR-GCP-PROJECTID'
                   , chunksize=10000,if_exists='append', progress_bar=False)
                   
    blob.delete()
    
    print("Blob {} deleted.".format(blob.name))
except:
    print('nothing here today ',date_time)
    pass
