import pandas as pd
import datetime
import pickle

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from prefect import flow, task,get_run_logger
from prefect.task_runners import SequentialTaskRunner

import prefect

@task
def read_data(path):
    df = pd.read_parquet(path)
    return df

@task
def prepare_features(df, categorical, train=True):
    logger = get_run_logger()
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    
    #logger = prefect.context.get("logger")
    if train:
        print(f"The mean duration of training is {mean_duration}")
        #logger.info(mean_duration)
        logger.info(f"The mean duration of training is {mean_duration}")
    else:
        print(f"The mean duration of validation is {mean_duration}")
        logger.info(f"The mean duration of training is {mean_duration}")
    
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df

@task
def train_model(df, categorical):
    
    logger = get_run_logger()

    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts) 
    y_train = df.duration.values

    #print(f"The shape of X_train is {X_train.shape}")
    logger.info(f"The shape of X_train is {X_train.shape}")

    #print(f"The DictVectorizer has {len(dv.feature_names_)} features")
    logger.info(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    #print(f"The MSE of training is: {mse}")
    logger.info(f"The MSE of training is: {mse}")
    return lr, dv

@task
def run_model(df, categorical, dv, lr):
    logger = get_run_logger()
    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts) 
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    #print(f"The MSE of validation is: {mse}")
    logger.info(f"The MSE of validation is: {mse}")
    return


    
@task
def get_paths(date):
    if date is not None:
        data=date
    else:
        date=datetime.date.today()
        date=str(date)
    data_dir="./data/fhv_tripdata_"
    filetype=".parquet"
    datee = datetime.datetime.strptime(date, "%Y-%m-%d")
    train_path=data_dir+str(datee.year)+"-"+str(datee.month-2).zfill(2)+filetype
    val_path=data_dir+str(datee.year)+"-"+str(datee.month-1).zfill(2)+filetype
    return train_path,val_path
    
    
    
    
@flow(task_runner=SequentialTaskRunner())
def main(date="2021-08-15"):
    
    train_path, val_path = get_paths(date).result()
    
    categorical = ['PUlocationID', 'DOlocationID']

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    
    
    if date is not None:
        data=date
    else:
        date=datetime.date.today()
        date=str(date)
    

    
    with open(f'models/model-{date}.bin', 'wb') as files:
        pickle.dump(lr, files)
        
    with open(f'models/dv-{date}.b', 'wb') as files:
        pickle.dump(dv, files)
    
    run_model(df_val_processed, categorical, dv, lr)
  

from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner
from datetime import timedelta

DeploymentSpec(
    flow=main,
    name="model_training_2021-08-15",
    schedule=CronSchedule(
        cron="0 9 15 * *",
        timezone="America/New_York"),
    flow_runner=SubprocessFlowRunner(),
    tags=["ml"]
)

