import os
import batch_intgration
import pandas as pd
from datetime import datetime


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

year = 2021
month = 1

input_file = f"s3://nyc-duration-jai/in/{year:04d}-{month:02d}.parquet"

data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]


columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
categorical = ['PUlocationID', 'DOlocationID']
df_input = pd.DataFrame(data, columns=columns)

url_endpoint = os.getenv("S3_ENDPOINT_URL")

options = {
'client_kwargs': {
    'endpoint_url': url_endpoint
    }
}

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)

os.system(f"cd .. && pipenv run python batch_intgration.py {year:04d} {month:02d}")

df = pd.read_parquet(f"s3://nyc-duration-jai/in/{year:04d}-{month:02d}.parquet", storage_options=options)

total_duration = sum(df["predicted_duration"])
print(total_duration)