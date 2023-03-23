from datetime import datetime

from pyspark import Row
from pyspark.sql import SparkSession

from pyfiles import insights, models


def test_get_num_records(spark: SparkSession):
    df = spark.createDataFrame([
        {"id": "id1", "name": "name1"},
        {"id": "id2", "name": "name2"},
        {"id": "id3", "name": "name3"},
        {"id": "id4", "name": "name4"},
    ])

    assert insights.get_num_records(df) == 4


def test_to_flattened(spark: SparkSession):
    df_raw = spark.createDataFrame([
        {
            "id": "1",
            "profile": {
                "firstName": "John",
                "lastName": "Doe",
                "jobHistory": [
                    {
                        "title": "Engineer",
                        "location": "Sydney",
                        "salary": 10,
                        "fromDate": datetime(2019, 3, 1)
                    }
                ]
            }
        },
        {
            "id": "2",
            "profile": {
                "firstName": "Jane",
                "lastName": "Doe",
                "jobHistory": [
                    {
                        "title": "PO",
                        "location": "Melbourne",
                        "salary": 10,
                        "fromDate": datetime(2020, 1, 1)
                    },
                    {
                        "title": "BA",
                        "location": "Sydney",
                        "salary": 8,
                        "fromDate": datetime(2012, 1, 1),
                        "toDate": datetime(2019, 12, 31)
                    }
                ]
            }
        },
        {
            "id": "3",
            "profile": {
                "firstName": "A",
                "lastName": "Jane",
                "jobHistory": []
            }
        }
    ], schema=models.get_schema())

    df = insights.to_flattened(df_raw)
    assert df.count() == 3

    row1 = Row(id="1", firstName="John", lastName="Doe", title="Engineer", location="Sydney",
               salary=10, fromDate=datetime(2019, 3, 1).date(), toDate=None)
    row2 = Row(id="2", firstName="Jane", lastName="Doe", title="PO", location="Melbourne",
               salary=10, fromDate=datetime(2020, 1, 1).date(), toDate=None)
    row3 = Row(id="2", firstName="Jane", lastName="Doe", title="BA", location="Sydney",
               salary=8, fromDate=datetime(2012, 1, 1).date(), toDate=datetime(2019, 12, 31).date())
    assert df.filter(df.id == "1").first() == row1
    assert df.filter((df.id == "2") & (df.title == "PO")).first() == row2
    assert df.filter((df.id == "2") & (df.title == "BA")).first() == row3


def test_get_avg_salary(spark: SparkSession):
    # case 1, calculate the average salary
    df = spark.createDataFrame([
        (10, "1"),
        (20, "2"),
        (15, "3"),
    ], schema="salary long, id string")

    assert insights.get_avg_salary(df) == 15.

    # case 2: to cover those without jobHistory, rows with salary=None will be ignored,
    # but 0 should be included
    df = spark.createDataFrame([
        (15, "1"),
        (20, "2"),
        (5, "3"),
        (None, "4"),
        (0, "5")
    ], schema="salary long, id string")

    assert insights.get_avg_salary(df) == 10.


def test_get_avg_salary_for_profile(spark: SparkSession):
    df_raw = spark.createDataFrame([
        ("1", "B", "Ab", 100),
        ("1", "B", "Ab", 20),
        ("2", "A", "Bc", 30),
        ("3", "D", "Cd", 100),
        ("3", "D", "Cd", 30),
        ("4", "C", "Ef", None),
    ], schema="id string, firstName string, lastName string, salary long")

    df = insights.get_avg_salary_for_profile(df_raw, 2)
    assert df.count() == 2
    assert df.filter(df.id == "1").count() == 0
    assert df.filter(df.id == "2").count() == 0
    p3 = df.filter(df.id == "3").collect()
    p4 = df.filter(df.id == "4").collect()

    assert len(p3) == 1
    assert p3[0].avg_salary == 65.

    assert len(p4) == 1
    assert p4[0].avg_salary is None


def test_get_top_avg_by_job(spark: SparkSession):
    df = spark.createDataFrame([
        ("1", "B", "Ab", 100, "BA"),
        ("1", "B", "Ab", 20, "PM"),
        ("2", "A", "Bc", 30, "BA"),
        ("2", "A", "Bc", 30, "IT"),
    ], schema="id string, firstName string, lastName string, salary long, title string")

    df_top2 = insights.get_top_avg_by_job(df, top_k=2, descending=True)
    df_bottom2 = insights.get_top_avg_by_job(df, top_k=2, descending=False)

    top2 = df_top2.collect()
    bottom2 = df_bottom2.collect()

    assert len(top2) == 2
    assert len(bottom2) == 2

    assert top2[0].title == "BA"
    assert top2[0].avg_salary == 65.
    assert top2[1].title == "IT"
    assert top2[1].avg_salary == 30.

    assert bottom2[0].title == "PM"
    assert bottom2[0].avg_salary == 20.
    assert bottom2[1].title == "IT"
    assert bottom2[1].avg_salary == 30.


def test_get_working_profiles(spark: SparkSession):
    df_raw = spark.createDataFrame([
        ("1", "B", "Ab", 100, "BA", datetime(2021, 2, 1), None),
        ("1", "B", "Ab", 20, "PM", datetime(2020, 1, 1), datetime(2021, 1, 1)),
        ("2", "A", "Bc", 30, "AB", datetime(2016, 1, 1), None),
        ("2", "A", "Bc", 30, "IT", datetime(2013, 1, 1), datetime(2016, 10, 1)),
    ], schema="id string, firstName string, lastName string, salary long, title string, fromDate date, toDate date")

    current_jobs = insights.get_working_profiles(df_raw).collect()
    assert len(current_jobs) == 2
    job1 = [v for v in current_jobs if v.id == "1"]
    job2 = [v for v in current_jobs if v.id == "2"]
    assert len(job1) == 1
    assert job1[0].title == "BA"
    assert len(job2) == 1
    assert job2[0].title == "AB"


def get_get_top_salary_profiles(spark: SparkSession):
    df_raw = spark.createDataFrame([
        ("1", 21, "Ab", datetime(2021, 2, 1)),
        ("1", 30, "Ab", datetime(2020, 2, 1)),
        ("1", 30, "Ab", datetime(2016, 12, 20)),
        ("2", 30, "Aa", datetime(2021, 2, 1)),
    ], schema="id string, salary long, lastName string, fromDate date")
    result = insights.get_top_salary_profiles(df_raw).collect()
    assert len(result) == 1
    assert result[0].id == "1"
    assert result[0].lastName == "Ab"
    assert result[0].fromDate == datetime(2020, 2, 1).date()


def test_get_popular_jobs(spark: SparkSession):
    df_raw = spark.createDataFrame([
        ("1", 21, "PM", datetime(2021, 2, 1)),
        ("1", 30, "BA", datetime(2020, 2, 1)),
        ("1", 30, "BA", datetime(2016, 12, 20)),
        ("1", 30, "BA", datetime(2013, 12, 20)),
        ("2", 30, "PM", datetime(2021, 2, 1))
    ], schema="id string, salary long, title string, fromDate date")

    popular1 = insights.get_popular_jobs(df_raw, started_in=2021).first()
    assert popular1.title == "PM"
    assert popular1.popularity == 2

    popular2 = insights.get_popular_jobs(df_raw, started_in=[2013, 2016, 2020, 2021]).first()
    assert popular2.title == "BA"
    assert popular2.popularity == 3

    top2 = insights.get_popular_jobs(df_raw, started_in=None, top_k=2).collect()
    popular3 = [v for v in top2 if v.title == "PM"][0]
    popular4 = [v for v in top2 if v.title == "BA"][0]
    assert popular3.title == "PM"
    assert popular3.popularity == 2
    assert popular4.title == "BA"
    assert popular4.popularity == 3


def test_get_latest_job_of_top_profile(spark: SparkSession):
    df_raw = spark.createDataFrame([
        ("1", "A", "Ca", datetime(2021, 2, 1)),
        ("1", "A", "Ca", datetime(2013, 2, 1)),
        ("2", "B", "Dc", datetime(2023, 2, 1)),
        ("2", "B", "Dc", datetime(2020, 2, 1)),
        ("3", "C", "Ab", datetime(2016, 12, 20)),
        ("4", "E", "Bc", None)
    ], schema="id string, firstName string, lastName string, fromDate date")

    df = insights.get_latest_job_of_top_profile(df_raw, top_k=2)
    assert df.count() == 2

    p1 = df.filter(df.id == "1").first()
    p2 = df.filter(df.id == "2").first()

    assert p1.fromDate == datetime(2021, 2, 1).date()
    assert p2.fromDate == datetime(2023, 2, 1).date()


def test_get_highest_salary_for_profiles(spark: SparkSession):
    df_raw = spark.createDataFrame([
        ("1", 30, datetime(2021, 2, 1)),
        ("1", 60, datetime(2013, 2, 1)),
        ("2", 11, datetime(2023, 2, 1)),
        ("2", 8, datetime(2020, 2, 1)),
        ("3", 3, datetime(2016, 12, 20)),
        ("4", None, None)
    ], schema="id string, salary long, fromDate date")

    df = insights.get_highest_salary_for_profiles(df_raw)
    assert df.count() == 4
    p1 = df.filter(df.id == "1").first()
    p2 = df.filter(df.id == "2").first()
    p3 = df.filter(df.id == "3").first()
    p4 = df.filter(df.id == "4").first()

    assert p1.salary == 60
    assert p1.year == 2013

    assert p2.salary == 11
    assert p2.year == 2023

    assert p3.salary == 3
    assert p3.year == 2016

    assert p4.salary is None
    assert p4.year is None
