FROM apache/spark-py:v3.3.2

ENV INPUT_PATH="data/"
ENV OUTPUT_PATH="output/"

WORKDIR /code

ADD . .

CMD /opt/spark/bin/spark-submit --py-files="pyfiles/" job_runner.py --input_path="${INPUT_PATH}" --output_path="${OUTPUT_PATH}"
