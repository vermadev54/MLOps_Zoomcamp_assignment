#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import datetime

from pathlib import Path


with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)




def read_data(filename,categorical):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def main(month,year):
    
    categorical = ['PUlocationID', 'DOlocationID']
    
    month = datetime.datetime.strptime(month, '%b').month


    input_file = f'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet'
    #output_file = f's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_file = f'taxi_type=fhv_year={year:04d}_month={month:02d}.parquet'
    
    df = read_data(input_file,categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    
    print(df_result)
    

    df_result.to_parquet(output_file, engine='pyarrow', index=False)
    
if __name__ == "__main__":
    main("feb", 2021)
    