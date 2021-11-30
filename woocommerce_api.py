from woocommerce import API
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import copy
from google.cloud import bigquery
import os
from pandas_gbq import read_gbq, to_gbq

def unnesting(df, explode, axis):
    if axis==1:
        idx = df.index.repeat(df[explode[0]].str.len())
        df1 = pd.concat([
            pd.DataFrame({x: np.concatenate(df[x].values)}) for x in explode], axis=1)
        df1.index = idx

        return df1.join(df.drop(explode, 1), how='left')
    else :
        df1 = pd.concat([
                         pd.DataFrame(df[x].tolist(), index=df.index).add_prefix(x) for x in explode], axis=1)
        return df1.join(df.drop(explode, 1), how='left')


def flattenDict(d,result=None,index=None,Key=None):
    if result is None:
        result = {}
    if isinstance(d, (list, tuple)):
        for indexB, element in enumerate(d):
            if Key is not None:
                newkey = Key
            flattenDict(element,result,index=indexB,Key=newkey)            
    elif isinstance(d, dict):
        for key in d:
            value = d[key]
            if Key is not None and index is not None:
                newkey = "_".join([Key,(str(key).replace(" ", ""))])
            elif Key is not None:
                newkey = "_".join([Key,(str(key).replace(" ", ""))])
            else:
                newkey= str(key).replace(" ", "")
            flattenDict(value,result,index=None,Key=newkey)
    else:
        result[Key]=d
    return result


def get_start_date():
    QUERY = ("""
            select cast(max(date_created) as date) as max_date
            from `YOUR-GCP-PROJECT.YOUR-BIGQUERY-DATASET.Orders`
            """)
    raw_data = read_gbq(QUERY, project_id = 'YOUR-GCP-PROJECT')
    raw_data2 = str(raw_data['max_date'].values[0]).split('T')[0]
    max_date = (datetime.strptime(raw_data2,"%Y-%m-%d")+ timedelta(days=1)).strftime('%Y-%m-%d')
    return max_date


def convertDate(d):
    new_date = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S")
    return new_date.date()

def upload_data_to_bq(KeyName):
    table_name = 'YOUR-BIGQUERY-DATASET.'+KeyName
    print(table_name)
    if KeyName in ['Customers']:
        dict_of_df[KeyName]['date_created']= pd.to_datetime(dict_of_df[KeyName]['date_created'])
        dict_of_df[KeyName]['date_modified']= pd.to_datetime(dict_of_df[KeyName]['date_modified'])
        to_gbq(dict_of_df[KeyName], table_name, project_id='YOUR-GCP-PROJECT'
               , chunksize=10000,if_exists='replace')
    else:
        #del dict_of_df['Orders']['_links_customer']
        dict_of_df[KeyName]['date_created']= pd.to_datetime(dict_of_df[KeyName]['date_created'])
        dict_of_df[KeyName]['date_modified']= pd.to_datetime(dict_of_df[KeyName]['date_modified'])
        dict_of_df[KeyName]['date_completed']= pd.to_datetime(dict_of_df[KeyName]['date_completed'])
        dict_of_df[KeyName]['date_paid']= pd.to_datetime(dict_of_df[KeyName]['date_paid'])
        dict_of_df[KeyName]['total']= dict_of_df[KeyName]['total'].astype(float)
        to_gbq(dict_of_df[KeyName], table_name, project_id='YOUR-GCP-PROJECT'
               , chunksize=10000,if_exists='append')

def pull_data_from_api(StartDate, DataSource):
    key_name = DataSource
    StartDate = StartDate
    EndDate = (datetime.now()- timedelta(days=3)).strftime('%Y-%m-%d')
    frame = []
    result = pd.DataFrame(None)
    if DataSource == 'Orders':
        while StartDate<=EndDate:
            StartDateTime = StartDate+'T00:00:00'
            EndDateTime = StartDate+'T23:59:59'
            cycle =1
            r= wcapi.get("orders/?after="+StartDateTime+"&before="+EndDateTime+"",)
            total_pages = int(r.headers['X-WP-TotalPages'])
            total_records = int(r.headers['X-WP-Total'])
            TotalCycles =int(total_records/100)+1
            print(StartDateTime,'-->Num Pages:',total_pages,'-->Total Records:' ,total_records )
            while cycle<=TotalCycles:
                print(cycle)
                string = "orders/?after="+StartDateTime+"&before="+EndDateTime+"&per_page=100&page="+str(cycle)
                order = wcapi.get(string).json()                          
                df = pd.json_normalize(order) #Results contain the required data
                frame.append(df)
                cycle+=1
        
            StartDate = datetime.strptime(StartDate,'%Y-%m-%d')+timedelta(days=1)
            StartDate = (StartDate).strftime('%Y-%m-%d')

    elif DataSource == 'Customers':
        r= wcapi.get("customers")
        total_pages = int(r.headers['X-WP-TotalPages'])
        total_records = int(r.headers['X-WP-Total'])
        print(total_pages)
        for i in range(1,total_pages+1):
            print(i)
            string = "customers/?per_page=10&page="+str(i)
            order = wcapi.get(string).json()                          
            df = pd.json_normalize(order) #Results contain the required data
            frame.append(df)
        
    result = pd.concat(frame).to_dict('records')
    result_2 = pd.DataFrame(result)
    result_2.columns=result_2.columns.str.replace('[#,@,&,-,=,+, ,:,.]','_')
    dict_of_df[key_name] = copy.deepcopy(result_2)
    upload_data_to_bq(key_name)
    return dict_of_df

wcapi = API(
    url="https://YOUR-WEBSITE.COM/",
    consumer_key="YOUR-CONSUMER-KEY",
    consumer_secret="YOUR-CONSUMER-SECRET",
    timeout=30
)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='LOCATION TO YOUR GCP CREDENTIALS'
client = bigquery.Client()

StartDate = get_start_date()
print(StartDate)
day = datetime.today().weekday()

if day ==1:
    DataSource = ['Orders','Customers']
else:
    DataSource = ['Orders']

dict_of_df = {}

for ds in DataSource:
    if StartDate>= (datetime.now()- timedelta(days=3)).strftime('%Y-%m-%d'):
            print('Data is up to date')
    else:
        dict_of_df = pull_data_from_api(StartDate, ds)
