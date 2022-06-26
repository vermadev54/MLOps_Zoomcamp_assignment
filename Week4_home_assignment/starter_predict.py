#!/usr/bin/env python
# coding: utf-8



import pickle
import pandas as pd
import sys

from datetime import datetime

import boto3
from botocore.exceptions import NoCredentialsError

with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)


categorical = ['PUlocationID', 'DOlocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def get_data_with_date(month,year):
    datetime_object1 = datetime.strptime(month,'%B')
    datetime_object2 = datetime.strptime(year,'%Y')
    
    filename= "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_"+ str(datetime_object2.year) + "-" + str(datetime_object1.month).zfill(2)+".parquet"
    #print(filename)
    
    df = read_data(filename)
    return df


ACCESS_KEY = 'XXXXXX'
SECRET_KEY = 'XXXXXX'


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful to s3")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False





def get_mean_predicted_duration(df):
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)
    
    year= datetime.today().year
    month=datetime.today().month
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df["prediction"]=y_pred
    df_result=df[["ride_id","prediction"]]
    df_result.to_parquet(
                "mlzoomcamp_OUTPUT",
                engine='pyarrow',
                compression=None,
                index=False
                )
    upload_to_aws('mlzoomcamp_OUTPUT', 'ml-code-store', '.')
    return(y_pred.mean())

def main():
    month = sys.argv[1]
    year = sys.argv[2]
    print(month , year)
    df_result = get_data_with_date(month,year)
    print(get_mean_predicted_duration(df_result))
    
main()

    
    
    





