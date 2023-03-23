FROM apache/spark-py:v3.3.2

WORKDIR /code

ADD . .

USER root
RUN pip install -r requirements-test.txt
USER 185

CMD PYTHONPATH=${PYTHONPATH}:$SPARK_HOME/python/lib/pyspark.zip:$(ls $SPARK_HOME/python/lib/py4j*.zip) pytest --cov=pyfiles/ tests/
