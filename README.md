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
