import unittest

from tasks.ingest import validate
from tasks.preprocess import transform as pre_transform
from tasks.process import transform as final_process
from utils import spark_config


class H1UnitTests(unittest.TestCase):
    def setUp(self):
        """
        Start Spark, define config and path to test data
        """
        self.spark = spark_config.get_spark_session('h1_test')
        self.test_raw_input = 'test_data/raw_data/input/'
        self.test_valid_data = 'test_data/raw_data/valid/'
        self.test_pre_processed_data = 'test_data/preprocessed_data/'
        self.test_process_output = 'test_data/processed_data/'

    def tearDown(self):
        """
        Stop Spark
        """
        self.spark.stop()

    def test_ingest(self):
        input_data = (
            self.spark
                .read.option('header', True)
                .csv(self.test_raw_input))

        expected_data = (
            self.spark
            .read.option('header', True)
            .csv(self.test_valid_data))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        valid_data = validate(input_data)

        cols = len(valid_data.columns)
        rows = valid_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in valid_data.columns])
        # assert sorted(expected_data.collect()) == sorted(valid_data.collect())

    def test_preprocess(self):
        input_data = (
            self.spark
                .read.option('header', True)
                .csv(self.test_valid_data))

        expected_data = (
            self.spark
            .read.option('header', True)
            .csv(self.test_pre_processed_data))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        pre_processed_data = pre_transform(input_data)

        cols = len(pre_processed_data.columns)
        rows = pre_processed_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in pre_processed_data.columns])
        # assert sorted(expected_data.collect()) == sorted(pre_processed_data.collect())

    def test_process(self):
        input_data = (
            self.spark
                .read.options(header='True', inferSchema='True', delimeter=',')
                .csv(self.test_pre_processed_data))

        expected_data = (
            self.spark
            .read.options(header='True', inferSchema='True', delimeter=',')
            .csv(self.test_process_output))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        processed_data = final_process(input_data)

        cols = len(processed_data.columns)
        rows = processed_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in processed_data.columns])
        assert sorted(expected_data.collect()) == sorted(processed_data.collect())

