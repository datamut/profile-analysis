import argparse

from pyspark import SparkContext


class Job:
    def __init__(self):
        pass

    @classmethod
    def run(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("--data_path", help="The path for the input data.")
        args = parser.parse_args()

        if not args.data_path:
            raise Exception("--data_path not found for the input data")

        data_path = args.data_path

        sc = SparkContext()
        rdd = sc.textFile(data_path)
        print(rdd.count())
