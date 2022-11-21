import unittest
import transform
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


class TransformTest(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls.spark = SparkSession.builder\
            .appName("testing_app")\
            .getOrCreate()

    def test_transform_by_genre_decade(self):
        df = self.spark.read\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .csv("dummy_data1.csv")
        expected_df = df.groupBy('decade', 'genre').count()
        transform_process = transform.Transform(self.spark)
        actual_df = transform_process.movie_by_decade(df)
        self.assertEqual(expected_df.collect(), actual_df.collect())


if __name__ == '__main__':
    unittest.main()
