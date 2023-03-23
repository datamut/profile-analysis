import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    This is a shared SparkSession across the tests. It minimises the overhead running the Spark locally.

    :return:
    """

    return (SparkSession
            .builder
            .master("local[1]")
            .getOrCreate())
