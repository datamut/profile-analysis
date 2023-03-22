from datetime import datetime
from typing import List

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as fun
from pyspark.sql.types import IntegerType

from pyfiles import insights
from pyfiles.utils import JobArgs


class Job:
    def __init__(self, args: List = None):
        self.job_args = JobArgs.parse_args(args)

    def run1(self):
        import json

        sc = SparkContext.getOrCreate()
        rdd = (sc
               .textFile(self.job_args.data_path)
               .map(lambda v: json.loads(v)["profile"]["jobHistory"])
               .flatMap(lambda v: v)
               .filter(lambda v: "toDate" not in v)
               )
        print(">>>>>>", rdd.take(10))
        print(f"Number of jobs: {rdd.count()}")

    def run(self):
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)

        # q1: Load the dataset into a Spark dataframe
        df_raw = spark.read.json(self.job_args.data_path, schema=insights.get_schema())

        # q2: Print the schema
        df_raw.printSchema()

        # q3: How many records are there in the dataset?
        num_records = df_raw.count()
        print(f"Total number of records: {num_records}")

        # convert the nested structure to flattened
        df = (df_raw
              .select("*", fun.col("profile.*"))
              .select("*", fun.explode("jobHistory").alias("jobHistoryTemp"))
              .select("*", fun.col("jobHistoryTemp.*"))
              .drop("profile", "jobHistory", "jobHistoryTemp")
              # Persist the flattened DataFrame improve the performance. But as the local laptop
              # doesn't have sufficient memory, choose the DISK_ONLY storage level to allow the
              # whole dataset to be able to run on the local laptop
              .persist(StorageLevel.DISK_ONLY))
        # show the flattened schema
        df.printSchema()

        # q4: What is the average salary for each profile?
        # Display the first 10 results, ordered by lastName in descending order.
        # TODO: also try the option to solve this without flatten
        top_avg_salary_by_profile = (df
                                     .groupBy("id", "firstName", "lastName")
                                     .avg("salary")
                                     .sort("lastName", ascending=False)
                                     .limit(2))
        print("The average salary for the first 10 profile, ordered by lastName descending:")
        top_avg_salary_by_profile.show(truncate=False)

        # q5: What is the average salary across the whole dataset?
        overall_avg_salary = df.agg(fun.avg(fun.col("salary")).alias("avg_salary"))
        print(f"The average salary across the whole dataset is:")
        overall_avg_salary.show(truncate=False)

        # q6: On average, what are the top 5 paying jobs? Bottom 5 paying jobs?
        # If there is a tie, please order by title, location.
        # TODO: how can we order by location given location is not a factor when calculating the average
        avg_by_job = df.groupBy("title").agg(fun.avg("salary").alias("avg_salary"))
        top_avg_by_job = avg_by_job.sort(fun.desc("avg_salary"), "title").limit(5)
        bottom_avg_by_job = avg_by_job.sort("avg_salary", "title").limit(5)
        print("Top 5 paying jobs are:")
        top_avg_by_job.show(truncate=False)
        print("Bottom 5 paying jobs are:")
        bottom_avg_by_job.show(truncate=False)

        # find out people which are currently working
        # each profile should have at most only one job in the jobHistory which is
        # currently working - no toDate, but to avoid any data issue, we only consider
        # the one with biggest fromDate and without toDate as the current job
        # TODO: get more clarify on this assumption
        window_from_date = Window.partitionBy("id").orderBy(fun.desc(fun.col("fromDate")))
        df_working = (df
                      .withColumn("rowId", fun.row_number().over(window_from_date))
                      .filter(fun.col("rowId") == 1)
                      .drop("rowId")
                      .filter(fun.isnull(fun.col("toDate")))
                      .persist(StorageLevel.DISK_ONLY))

        # q9: How many people are currently working?
        print(f"There are {df_working.count()} are currently working")

        # q7: Who is currently making the most money?
        # If there is a tie, please order in lastName descending, fromDate descending.
        current_highest_salary = (df_working
                                  .sort(fun.desc(fun.col("salary")),
                                        fun.desc(fun.col("lastName")),
                                        fun.desc(fun.col("fromDate")))
                                  .limit(1))
        print("The people currently making the most money is:")
        current_highest_salary.show(truncate=False)

        df_working.unpersist()

        # q8: What was the most popular job title that started in 2019?
        # TODO: this will assume that jobs which not end by 2019-01-01 is count 1 for the popular score
        popular_jobs = (df
                        .filter((df.toDate >= "2019-01-01") | fun.isnull(df.toDate))
                        .groupBy("title")
                        .count()
                        .sort(fun.desc("count"))
                        .limit(1))
        popular_jobs.show(truncate=False)

        # q10 For each person, list only their latest job. Display the first 10 results,
        # ordered by lastName descending, firstName ascending order.
        latest_jobs = (df
                       .withColumn("rowId", fun.row_number().over(window_from_date))
                       .filter(fun.col("rowId") == 1)
                       .drop("rowId")
                       .sort(fun.desc(fun.col("lastName")), "lastName")
                       .limit(10))
        print("The 10 people with their latest jobs:")
        latest_jobs.show(truncate=False)

        # q11: For each person, list their highest paying job along with their first name, last name,
        # salary and the year they made this salary. Store the results in a dataframe, and then print out 10 results
        # TODO: the `year` people made the highest salary could be across multiple years,
        #  assume we take the year of the `fromDate`
        udf_year = fun.udf(lambda x: x.year, IntegerType())
        window_salary = Window.partitionBy("id").orderBy(fun.desc(fun.col("salary")))
        df_top_salary = (df
                         .withColumn("rowId", fun.row_number().over(window_salary))
                         .filter(fun.col("rowId") == 1)
                         .drop("rowId")
                         .withColumn("year", udf_year("fromDate"))
                         .persist(StorageLevel.DISK_ONLY))
        df_top_salary.show(10, truncate=False)

        # q12: Write out the last result (question 11) in parquet format, compressed,
        # partitioned by the year of their highest paying job.
        # TODO: parameterise the output path
        (df_top_salary
         .write
         .format("parquet")
         .partitionBy("year")
         .save("data/output/1"))

        df.unpersist(True)
