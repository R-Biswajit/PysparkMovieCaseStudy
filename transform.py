import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *

def row_number():
    pass

class Transform:

    def __init__(self, spark):
        self.spark = spark

    def join_movie_data(self, df1, df2):
        print("transformed join on movie and item data")

        join_df1 = df1.join(df2, df1.movieId == df2.itemId).drop(df2.itemId)
        return join_df1

    def join_user_movie_data(self, df1, df2):
        print("transformed join on user and joined movie data item")
        join_df2 = df1.join(df2, df1.userId == df2.userId).drop(df2.userId)
        join_df3 = join_df2.select('UserId', 'age', 'movieId', 'genre')
        return join_df3

    def highest_rating(self, df):
        print("print highest rating")
        hdf1 = df.withColumn("rank", f.dense_rank().over(Window.partitionBy("genre").orderBy(f.col("rating").desc())))
        hdf2 = hdf1.filter(hdf1.rank == 1)
        return hdf2

    def lowest_rating(self, df):
        print("print lowest rating")
        ldf1 = df.withColumn("rank", f.dense_rank().over(Window.partitionBy("genre").orderBy("rating")))
        ldf2 = ldf1.filter(ldf1.rank == 1)
        return ldf2

    def movie_by_decade(self, df):
        print("print movie by decade")
        df_decade = df.groupBy('decade', 'genre').count()
        return df_decade

    def movie_by_age(self, df):
        print("print movie by age")
        df_age = df.filter(f.col('age').between(40, 45)).groupBy('genre').count()
        return df_age
