Profile Analysis
----

This is a basic profile analysis project.

## Environment

This project is built and tested on:

- Spark 3.3.2
- Python 3.9.2
- Docker 4.17.0

## Run

To run a given dataset, please follow the below steps:

### 1. Build the docker image

```shell
docker build -t profile-analysis .
```

### 2. Run with your dataset path

```shell
docker run -it --rm -v /your/path/to/test_data:/data/input -v /your/output/path:/data/output -e INPUT_PATH=/data/input/ -e OUTPUT_PATH=/data/output/1 profile-analysis
```

Note:

+ (1) You can mount the dataset path `/your/path/to/test_data` from your host/local as `/data/` inside the docker container
+ (2) Specify the `-e INPUT_PATH=/data/` as the mount path in the docker container
+ (3) You can also specify the `OUTPUT_PATH` similar with the input_path, by saving the output to you host/local instead of only storing in the container

Then you can run the job using your local/own dataset.

You can also run with the mock data included without specifying the INPUT_PATH parameter like below:

```shell
docker run -it --rm profile-analysis
```

Note that the mock data is a masked sample dataset based on the given test data (even the given test data should be already masked).

## Unit Tests

You can simply run the unit tests with coverage with two simple steps:

### 1. Build the docker image for unit tests

```shell
docker build -t profile-analysis-test -f test.Dockerfile .
```

### 2. Run the unit tests with coverage

```shell
docker run -it --rm profile-analysis-test
```


## CI/CD

## Assumptions made for questions

+ Question 6: On average, what are the top 5 paying jobs? Bottom 5 paying jobs? If there is a tie, please order by title, location.

It requires to find the top 5 and bottom 5 paying jobs based the average salary. From the given dataset, we can (only) use the title as the identifier of a job. The same job could be across multiple locations, so it should be not feasible to order by location if there is a tie.

+ Question 8: What was the most popular job title that started in 2019?

For this question, I have two assumptions regarding how to define "popularity" of a job, and how to interpret "started in 2019".
Firstly, I assume that the popularity scored 1 when the job title appears once in a person's job history, without considering the period the person working on that job.
Secondly, regarding "started in 2019", I assume that if the fromDate of a job is between [2019-01-01, 2020-01-01), it will be counted.

For example, given the below job history:

| title | fromDate   | toDate     | popularity | comment                    |
| ---- |------------|------------|------------|----------------------------|
| data engineer | 2015-03-01 | 2019-01-01 | 0          | started before 2019-01-01  |
| data engineer | 2019-02-01 | 2019-09-01 | 1          | started in 2019 |
| software engineer | 2019-01-01 | null       | 1          | started in 2019      |

from the above example, both data engineer and software engineer will both have popularity score as 1.

+ Question 11: For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary. Store the
results in a dataframe, and then print out 10 results

The year is not part of the original data, but we have fromDate and toDate. I made an assumption to use the year of the fromDate as the `year`. And question 12 will be partitioned by this `year`.
