import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql.types import *


class Extract:

    def __init__(self, spark):
        self.spark = spark

    def extract_movie_item(self):
        print("extracting movie item")
        schema = StructType([StructField("movieId", IntegerType()),
                             StructField("title", StringType()),
                             StructField("releaseDate", StringType()),
                             StructField("videoReleaseDate", StringType()),
                             StructField("URL", StringType()),
                             StructField("unknown", StringType()),
                             StructField("action", StringType()),
                             StructField("adventure", StringType()),
                             StructField("animation", StringType()),
                             StructField("children", StringType()),
                             StructField("comedy", StringType()),
                             StructField("crime", StringType()),
                             StructField("documentary", StringType()),
                             StructField("drama", StringType()),
                             StructField("fantasy", StringType()),
                             StructField("filmnoir", StringType()),
                             StructField("horror", StringType()),
                             StructField("musical", StringType()),
                             StructField("mistory", StringType()),
                             StructField("romance", StringType()),
                             StructField("SciFi", StringType()),
                             StructField("thriller", StringType()),
                             StructField("war", StringType()),
                             StructField("western", StringType())])

        item_df = self.spark.read \
            .schema(schema) \
            .option("delimiter", "|") \
            .csv("D:\demo\case_study\movielense_analysis\input\item.txt")

        item_df1 = item_df.drop('releaseDate', 'videoReleaseDate', 'URL')

        item_df2 = item_df1.withColumn("unknown", f.when(f.col("unknown") == f.lit("1"), f.lit("unknown"))\
                  .otherwise("null")).withColumn("action", f.when(f.col("action") == f.lit("1"), f.lit("action"))\
                  .otherwise("null")).withColumn("adventure", f.when(f.col("adventure") == f.lit("1"), f.lit("adventure"))\
                  .otherwise("null")).withColumn("animation", f.when(f.col("animation") == f.lit("1"), f.lit("animation"))\
                  .otherwise("null")).withColumn("children", f.when(f.col("children") == f.lit("1"), f.lit("children"))\
                  .otherwise("null")).withColumn("comedy", f.when(f.col("comedy") == f.lit("1"), f.lit("comedy"))\
                  .otherwise("null")).withColumn("crime", f.when(f.col("crime") == f.lit("1"), f.lit("crime"))\
                  .otherwise("null")).withColumn("documentary", f.when(f.col("documentary") == f.lit("1"), f.lit("documentary"))\
                  .otherwise("null")).withColumn("drama", f.when(f.col("drama") == f.lit("1"), f.lit("drama"))\
                  .otherwise("null")).withColumn("fantasy", f.when(f.col("fantasy") == f.lit("1"), f.lit("fantasy"))\
                  .otherwise("null")).withColumn("filmnoir", f.when(f.col("filmnoir") == f.lit("1"), f.lit("filmnoir"))\
                  .otherwise("null")).withColumn("horror", f.when(f.col("horror") == f.lit("1"), f.lit("horror"))\
                  .otherwise("null")).withColumn("musical", f.when(f.col("musical") == f.lit("1"), f.lit("musical"))\
                  .otherwise("null")).withColumn("mistory", f.when(f.col("mistory") == f.lit("1"), f.lit("mistory"))\
                  .otherwise("null")).withColumn("romance", f.when(f.col("romance") == f.lit("1"), f.lit("romance"))\
                  .otherwise("null")).withColumn("SciFi", f.when(f.col("SciFi") == f.lit("1"), f.lit("SciFi"))\
                  .otherwise("null")).withColumn("thriller", f.when(f.col("thriller") == f.lit("1"), f.lit("thriller"))\
                  .otherwise("null")).withColumn("war", f.when(f.col("war") == f.lit("1"), f.lit("war"))\
                  .otherwise("null")).withColumn("western", f.when(f.col("western") == f.lit("1"), f.lit("western"))\
                  .otherwise("null"))


        item_df2 = item_df2.withColumn("genre",f.concat_ws(",",f.col("unknown"),f.col("action"),f.col("adventure"),\
                                                          f.col("animation"),f.col("children"),f.col("comedy"),\
                                                          f.col("crime"),f.col("documentary"),f.col("drama"),\
                                                          f.col("fantasy"),f.col("filmnoir"),f.col("horror"),\
                                                          f.col("musical"),f.col("mistory"),f.col("romance"),\
                                                          f.col("SciFi"),f.col("thriller"),f.col("war"),\
                                                          f.col("western")))

        item_df3 = item_df2.withColumn("genre", f.split(f.col("genre"), ","))
        item_df4 = item_df3.select(item_df3.movieId, item_df3.title, f.explode(item_df3.genre).alias("genre"))
        item_df5 = item_df4.filter(f.col("genre") != "null")
        item_df6 = item_df5 .select('movieId', 'title', 'genre', f.regexp_extract('title', r'\((\d+)\)', 1).alias('year'))
        item_df7 = item_df6.withColumn('decade', f.substring('year', 1, 3)).drop('year')
        return item_df7



    def extract_data_item(self):
        print("extracting data item")
        data_df = self.spark.read.text("D:\demo\case_study\movielense_analysis\input\data.txt")
        data_df = data_df.withColumn("splitable", f.split("value", "\t"))
        data_df = data_df.withColumn("userId", f.col("splitable")[0].cast('integer'))\
            .withColumn("itemId", f.col("splitable")[1].cast('integer'))\
            .withColumn("rating", f.col("splitable")[2].cast('integer'))\
            .withColumn("timestamp", f.col("splitable")[3].cast('integer'))
        data_df = data_df .select("userId", "itemId", "rating", "timestamp")
        return data_df


    def extract_user_info(self):
        print("extracting user information")

        user_schema = StructType([StructField("userId", IntegerType()),
                                  StructField("age", IntegerType()),
                                  StructField("gender", StringType()),
                                  StructField("occupation", StringType()),
                                  StructField("zipCode", IntegerType())])

        user_df = self.spark.read \
            .schema(user_schema) \
            .option("delimiter", "|") \
            .csv("D:\demo\case_study\movielense_analysis\input\infouser.txt")
        return user_df

