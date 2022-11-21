import pyspark
from pyspark.sql import SparkSession


class Load:

    def __init__(self, spark):
        self.spark = spark

    def load_highest_rating(self, df):
        print("loading highest rating")
        df.coalesce(1)\
            .write \
            .mode("overwrite")\
            .option("delimiter", "|")\
            .option("header", True)\
            .csv("D:\demo\case_study\movielense_analysis\output\highest")

    def load_lowest_rating(self, df):
        print("loading lowest rating")
        df.coalesce(1)\
            .write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", True) \
            .csv("D:\demo\case_study\movielense_analysis\output\lowest")

    def load_movie_by_decade(self, df):
        print("loading movie by decade and genre")
        df.coalesce(1)\
            .write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", True) \
            .csv("D:\demo\case_study\movielense_analysis\output\decade")

    def load_movie_by_age(self, df):
        print("loading movie by age and genre")
        df.coalesce(1)\
            .write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", True) \
            .csv("D:\demo\case_study\movielense_analysis\output\genre_age")
