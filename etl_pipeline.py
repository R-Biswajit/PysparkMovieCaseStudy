import pyspark
from pyspark.sql import SparkSession

import extract
import transform
import load


class Pipeline:

    def run_pipeline(self):
        print("running etl pipeline")
        extract_process = extract.Extract(self.spark)
        movie_df = extract_process.extract_movie_item()
        data_df = extract_process.extract_data_item()
        user_df = extract_process.extract_user_info()

        transform_process = transform.Transform(self.spark)
        join_df1 = transform_process.join_movie_data(movie_df, data_df)
        join_df2 = transform_process.join_user_movie_data(user_df, join_df1)
        highest_rating_df = transform_process.highest_rating(join_df1)
        lowest_rating_df = transform_process.lowest_rating(join_df1)
        movie_df_decade = transform_process.movie_by_decade(join_df1)
        movie_df_age = transform_process.movie_by_age(join_df2)

        load_process = load.Load(self.spark)
        load_process.load_highest_rating(highest_rating_df)
        load_process.load_lowest_rating(lowest_rating_df)
        load_process.load_movie_by_decade(movie_df_decade)
        load_process.load_movie_by_age(movie_df_age)
        return
    # Initiate Spark application and get Spark Session
    def create_spark_session(self):
        self.spark = SparkSession.builder \
            .appName("MovieLensesAnalysis") \
            .getOrCreate()

# from main function call run_pipeline method
if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
