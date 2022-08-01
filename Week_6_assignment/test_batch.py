from pathlib import Path
import pandas as pd
from datetime import datetime
import pickle
from pandas.testing import assert_frame_equal

from deepdiff import DeepDiff


with open('model.bin', 'rb') as f_in:
    dv, lr = pickle.load(f_in)


def read_data(filename,categorical):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def prepare_data():
    filename = 'tests/input_unit_test.parquet'
    expected_output_file='tests/excepted_output.parquet'
    now_time= datetime.now()
    month = now_time.month
    year = now_time.year
    
    categorical = ['PUlocationID', 'DOlocationID']
    df = read_data(filename,categorical)
    
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())

    actual_result = pd.DataFrame()
    actual_result['ride_id'] = df['ride_id']
    actual_result['predicted_duration'] = y_pred 
    expected_result=pd.read_parquet(expected_output_file)
    #expected_result=expected_result['predicted_duration']
    print("actual_result",actual_result)
    print("expected_result",expected_result)
    
    diff = DeepDiff(actual_result.to_dict(), expected_result.to_dict(), significant_digits=1)
    
    print(f'diff={diff}')

    assert 'type_changes' not in diff
    assert 'values_changed' not in diff
    
    
    
    assert actual_result.to_dict() == expected_result.to_dict()
    #assert assert_frame_equal(actual_result, expected_result,check_dtype=False)

prepare_data()
    