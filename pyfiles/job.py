import time
from typing import List

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession

from pyfiles import insights
from pyfiles.models import get_schema
from pyfiles.utils import JobArgs


class Job:
    def __init__(self, args: List = None):
        self.job_args = JobArgs.parse_args(args)

    def run(self):
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("ERROR")
        spark = SparkSession(sc)

        time_start = time.time()
        # q1: Load the dataset into a Spark dataframe
        df_raw = spark.read.json(self.job_args.input_path, schema=get_schema())

        print("---------The following are the result for each of the questions----------")

        # q2: Print the schema
        print("Answer 2 (1/2): Print the raw data schema:")
        df_raw.printSchema()

        # q3: How many records are there in the dataset?
        print(f"Answer 3: Total number of records in the dataset: {insights.get_num_records(df_raw)}")

        # convert the nested structure to flattened
        # Persist the flattened DataFrame to improve the performance. But as the local laptop
        # doesn't have sufficient memory, we choose to use the DISK_ONLY storage level to allow the
        # whole dataset to be able to run on the local laptop. Normally, we should be able to run
        # this using the MEMORY_AND_DISK level to boost the performance
        df = insights.to_flattened(df_raw).persist(StorageLevel.DISK_ONLY)

        # show the new schema for the flattened DataFrame, this will be the DataFrame for the rest of the work
        print("Answer 2 (2/2): Print the flattened data schema:")
        df.printSchema()

        # q4: What is the average salary for each profile?
        # Display the first 10 results, ordered by lastName in descending order.
        top_avg_salary_for_profile = insights.get_avg_salary_for_profile(df)
        print("Answer 4: The average salary for the first 10 profiles, ordered by lastName descending:")
        top_avg_salary_for_profile.show(truncate=False)

        # q5: What is the average salary across the whole dataset?
        print(f"Answer 5: The average salary across the whole dataset is: {insights.get_avg_salary(df)}")

        # q6: On average, what are the top 5 paying jobs? Bottom 5 paying jobs?
        # If there is a tie, please order by title, location.
        # TODO: how can we order by location given location is not a factor when calculating the average
        top_avg_by_job = insights.get_top_avg_by_job(df)
        bottom_avg_by_job = insights.get_top_avg_by_job(df, descending=False)
        print("Answer 6 (1/2): top paying jobs are:")
        top_avg_by_job.show(truncate=False)
        print("Answer 6 (2/2): bottom paying jobs are:")
        bottom_avg_by_job.show(truncate=False)

        # Find out people which are currently working
        df_working = insights.get_working_profiles(df).persist(StorageLevel.DISK_ONLY)

        # q9: How many people are currently working?
        print(f"Answer 9: There are {df_working.count()} people are currently working")

        # q7: Who is currently making the most money?
        # If there is a tie, please order in lastName descending, fromDate descending.
        print("Answer 7: The people currently making the most money is:")
        insights.get_top_salary_profiles(df_working).show(truncate=False)

        # un-persist the df_working, to release some space
        df_working.unpersist()

        # q8: What was the most popular job title that started in 2019?
        print("Answer 8: The most popular job started in 2019 is:")
        insights.get_popular_jobs(df).show(truncate=False)

        # q10 For each person, list only their latest job. Display the first 10 results,
        # ordered by lastName descending, firstName ascending order.
        print("Answer 10: The 10 people with their latest jobs:")
        insights.get_latest_job_of_top_profile(df).show(truncate=False)

        # q11: For each person, list their highest paying job along with their first name, last name,
        # salary and the year they made this salary. Store the results in a dataframe, and then print out 10 results
        df_top_salary = insights.get_highest_salary_for_profiles(df).persist(StorageLevel.DISK_ONLY)
        print("Answer 11: The 10 people with their jobs of highest salary:")
        df_top_salary.show(10, truncate=False)

        # q12: Write out the last result (question 11) in parquet format, compressed,
        # partitioned by the year of their highest paying job.
        insights.save_dataframe(df_top_salary, self.job_args.output_path)
        print(f"Answer 12: The DataFrame is stored in {self.job_args.output_path}")

        df_top_salary.unpersist()
        df.unpersist(True)

        print(f"The overall time used to process the data: {time.time() - time_start} seconds")
