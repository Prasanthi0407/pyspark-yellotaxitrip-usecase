"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
import json
from dependencies.spark import start_spark
from jobs.etl_job import transform_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{"startDate": "2020-01-01"}""")
        self.spark, *_ = start_spark()
        self.test_data_path = 'test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'input'))

        expected_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'output'))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        # act
        data_transformed = transform_data(input_data)

        cols = len(expected_data.columns)
        rows = expected_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()
