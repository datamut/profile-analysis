Profile Analysis
----

This is a basic profile analysis project.

## Environment

This project is built and tested on:

- Spark 3.3.2
- Python 3.9.2
- Docker 4.17.0

## Run

To run a given dataset, we will need:

1. Build the docker image

```shell
docker build -t profile-analysis .
```

2. Run with your dataset path

```shell
docker run -it --rm -v /your/path/to/test_data:/data -e DATA_PATH=/data/ profile-analysis
```

Note:
(1) You can mount the dataset path `/your/path/to/test_data` from your host/local as `/data/` inside the docker container
(2) Specify the `-e DATA_PATH=/data/` as the mount path in the docker container
Then you can run the job using your local/own dataset.

You can also run with the mock data included without specifying the DATA_PATH parameter like below:

```shell
docker run -it --rm profile-analysis
```

Note that the mock data is a masked sample dataset based on the given test data (even the given test data should be already masked).

## Unit Tests

## CI/CD

## Assumptions made for questions

+ Question 6: On average, what are the top 5 paying jobs? Bottom 5 paying jobs? If there is a tie, please order by title, location.

It requires to find the top 5 and bottom 5 paying jobs based the average salary. From the given dataset, we can (only) use the title as the identifier of a job. The same job could be across multiple locations, so it should be not feasible to order by location if there is a tie.

+ Question 8: What was the most popular job title that started in 2019?

For this question, I have two assumptions regarding how to define "popularity" of a job, and how to interpret "started in 2019".
Firstly, I assume that the popularity scored 1 when the job title appears once in a person's job history, without considering the period the person working on that job.
Secondly, regarding "started in 2019", I assume that if a job is not ended before 2019-01-01, it will be counted.

For example, given the below job history:

| title | fromDate | toDate | popularity | comment                                     |
| ---- | ---- | ---- | ---- |---------------------------------------------|
| data engineer | 2015-03-01 | 2019-01-01 | 1 | edge case, still not end before 2019-01-01  |
| data engineer | 2016-02-01 | 2018-09-01 | 0 | end before 2019-01-01                       |
| software engineer | 2020-01-01 | 2022-01-01 | 1 | start and end after 2019-01-01              |

from the above example, both data engineer and software engineer will both have popularity score as 1.

+ Question 11: For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary. Store the
results in a dataframe, and then print out 10 results

The year is not part of the original data, but we have fromDate and toDate. I made an assumption to use the year of the fromDate as the `year`. And question 12 will be partitioned by this `year`.
