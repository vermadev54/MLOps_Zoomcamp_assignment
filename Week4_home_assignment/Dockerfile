FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

RUN pip install -U pip
RUN pip install pipenv 
RUN pip install pyarrow
RUN pip install boto3
#RUN pip install -r requirements.txt

WORKDIR /app

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY [ "starter_predict.py", "./" ]

CMD ["python","starter_predict.py", "April", "2021"]

#CMD ["aws", "s3" "cp", "mlzoomcamp_OUTPUT" "s3://ml-code-store/"]
#RUN aws s3 cp mlzoomcamp_OUTPUT s3://ml-code-store/
