from pyspark.sql import SparkSession

from pyfiles.insights import get_num_records


def test_get_num_records(spark: SparkSession):
    df = spark.createDataFrame([
        {"id": "id1", "name": "name1"},
        {"id": "id2", "name": "name2"},
        {"id": "id3", "name": "name3"},
        {"id": "id4", "name": "name4"},
    ])

    assert get_num_records(df) == 4
