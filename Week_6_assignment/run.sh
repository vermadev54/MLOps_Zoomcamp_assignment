#!/usr/bin/env bash

cd "$(dirname "$0")"

docker-compose up -d

sleep 5

export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

export S3_ENDPOINT_URL="http://localhost:4566"

export INPUT_FILE_PATTERN="s3://nyc-duration-jai/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration-jai/out/{year:04d}-{month:02d}.parquet"

aws s3 mb s3://nyc-duration-jai --endpoint-url=${S3_ENDPOINT_URL}

pipenv run python integration_test.py

aws --endpoint-url=${S3_ENDPOINT_URL} s3 ls

aws s3 ls s3://nyc-duration-jai/in/ --endpoint-url=http://localhost:4566

aws s3 ls s3://nyc-duration-jai/out/ --endpoint-url=http://localhost:4566

# aws --endpoint-url=${S3_ENDPOINT_URL}  s3 cp ${INPUT/_FILE_PATTERN} .

ERROR_CODE=$?

docker-compose down

exit ${ERROR_CODE}