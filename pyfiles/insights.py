from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DateType


def get_schema() -> StructType:
    # lock the schema, instead of infer the schema from the data, to avoid any unnoticed schema changing
    return StructType([
        StructField("id", StringType(), False),
        StructField("profile", StructType([
            StructField("firstName", StringType(), False),
            StructField("lastName", StringType(), False),
            StructField("jobHistory", ArrayType(StructType([
                StructField("title", StringType(), False),
                StructField("location", StringType(), False),
                StructField("salary", LongType(), False),
                StructField("fromDate", DateType(), False),
                StructField("toDate", DateType(), True)
            ]), False), False)
        ]), False)
    ])
