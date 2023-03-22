FROM apache/spark-py:v3.3.2

ENV DATA_PATH="data/"

WORKDIR /code

ADD . .

CMD /opt/spark/bin/spark-submit --py-files="pyfiles/" job_runner.py --data_path="${DATA_PATH}"
