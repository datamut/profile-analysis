from typing import Union, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import IntegerType


def get_num_records(df: DataFrame) -> int:
    """
    Simply return the number of records in the DataFrame
    :param df:
    :return:
    """

    return df.count()


def to_flattened(df: DataFrame) -> DataFrame:
    """
    This function will convert the nested profile DataFrame into a flattened format.
    Note that this is only works for the profile dataset, instead of a general purpose transformation.
    The original profile, profile.jobHistory will be dropped from the new DataFrame, and all nested
    fields will become top level fields.

    :param df:
    :return:
    """

    return (df
            .select("*", F.col("profile.*"))
            .select("*", F.explode("jobHistory").alias("jobHistoryTemp"))
            .select("*", F.col("jobHistoryTemp.*"))
            .drop("profile", "jobHistory", "jobHistoryTemp"))


def get_avg_salary(df: DataFrame) -> float:
    """
    Get the average salary across all profiles and all job history.

    :param df:
    :return: The average salary
    """

    col_alias = "avg_salary"
    return df.agg(F.avg(F.col("salary")).alias(col_alias)).collect()[0][col_alias]


def get_avg_salary_for_profile(df: DataFrame, top_k: int = 10) -> DataFrame:
    """
    Calculate the average salary for each of the person/profile, based on their job history.
    Only return the first `top_k` records, order by `lastName` desc.

    :param df:
    :param top_k:
    :return:
    """

    return (df
            .groupBy("id", "firstName", "lastName")
            .avg("salary")
            .sort("lastName", ascending=False)
            .limit(top_k))


def get_top_avg_by_job(df: DataFrame, top_k: int = 5, descending: bool = True) -> DataFrame:
    """
    Get the top N jobs based on their average salary. Depending on the descending=True|False,
    we can get the top ones and bottom ones.

    :param df:
    :param top_k:
    :param descending:
    :return:
    """

    avg_by_job = df.groupBy("title").agg(F.avg("salary").alias("avg_salary"))
    if not descending:
        # get the bottom N jobs based on the average salary
        return avg_by_job.sort("avg_salary", "title").limit(top_k)

    # get the top N jobs based on the average salary
    return avg_by_job.sort(F.desc("avg_salary"), "title").limit(top_k)


def get_working_profiles(df: DataFrame) -> DataFrame:
    """
    Find the profiles which are currently working (with toDate field absent - null in the DataFrame).

    Find out profiles which are currently working (with toDate field absent - null in the DataFrame).
    Each profile should have at most only one job in the jobHistory which is currently working,
    but in order to avoid any data issue, we only consider the one with nearest fromDate and without toDate
    as the current job.

    If a profile has an earlier job which was not ended, but it has a latest job which is ended, we consider
    this profile as not currently working.

    TODO: discuss about this assumption, and get more clarifications

    :param df:
    :return:
    """

    window = Window.partitionBy("id").orderBy(F.desc(F.col("fromDate")))

    return (df
            .withColumn("rowId", F.row_number().over(window))
            .filter(F.col("rowId") == 1)
            .drop("rowId")
            .filter(F.isnull(F.col("toDate"))))


def get_top_salary_profiles(df: DataFrame, top_k: int = 1) -> DataFrame:
    """
    Get the profile which making the highest salary, if there is a tie, sort by lastName desc and fromData desc.

    :param df:
    :param top_k:
    :return:
    """

    return (df
            .sort(F.desc(F.col("salary")),
                  F.desc(F.col("lastName")),
                  F.desc(F.col("fromDate")))
            .limit(top_k))


def get_popular_jobs(df: DataFrame, started_in: Union[int, List[int]] = 2019, top_k: int = 1) -> DataFrame:
    """
    Get the most popular jobs based on how many times people working on the job. We only consider occurrence so far,
    without considering the period people working on that job.

    # TODO: discuss the assumption, see README.md for details about the assumptions

    :param df:
    :param started_in:
    :param top_k:
    :return:
    """

    if started_in:
        udf_year = F.udf(lambda x: x.year, IntegerType())
        started_in = [started_in] if isinstance(started_in, int) else started_in
        df = (df
              .withColumn("fromYear", udf_year("fromDate"))
              .filter(F.col("fromYear").isin(started_in))
              .drop("fromYear"))

    return (df
            .groupBy("title")
            .agg(F.count("title").alias("popularity"))
            .sort(F.desc("popularity"))
            .limit(top_k))


def get_latest_job_of_top_profile(df: DataFrame, top_k: int = 10) -> DataFrame:
    """
    Get the latest job for each of the profiles, and only return top N of those profiles with the latest job.

    :param df:
    :param top_k:
    :return:
    """

    window = Window.partitionBy("id").orderBy(F.desc(F.col("fromDate")))

    return (df
            .withColumn("rowId", F.row_number().over(window))
            .filter(F.col("rowId") == 1)
            .drop("rowId")
            .sort(F.desc(F.col("lastName")), "firstName")
            .limit(top_k))


def get_highest_salary_for_profiles(df: DataFrame) -> DataFrame:
    """
    Get the highest salary for each profile.

    # TODO: the `year` people made the highest salary could be across multiple years,
    #  assume we take the year of the `fromDate`

    :param df:
    :return:
    """

    udf_year = F.udf(lambda x: x.year, IntegerType())
    window = Window.partitionBy("id").orderBy(F.desc(F.col("salary")))
    return (df
            .withColumn("rowId", F.row_number().over(window))
            .filter(F.col("rowId") == 1)
            .drop("rowId")
            .withColumn("year", udf_year("fromDate")))


def save_dataframe(df: DataFrame, output_path: str):
    """
    Save the DataFrame as parquet format, and partition by `year`

    :param df:
    :param output_path:
    :return:
    """

    (df
     .write
     .partitionBy("year")
     .mode("error")
     .parquet(output_path, compression="snappy"))
