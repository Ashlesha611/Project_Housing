import unittest
from pyspark.sql import SparkSession

class TestCreateDeltaTableJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession object
        builder = (
          pyspark.sql.SparkSession.builder.appName("Delta_Table")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
              )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()


        # Define the path to the Delta table
        cls.delta_table_path = "/path/to/housing_dataset_delta"

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession object
        cls.spark.stop()

    def test_create_delta_table_job(self):
        """
        Test that the job creates the Delta table with the expected schema.
        """
        # Run the job
        create_delta_table_job()

        # Check that the Delta table has the expected schema
       
        table = self.spark.table("housing-dataset")
        expected_columns = set(['Longitude','Latitude','housingMedianAge','totalRooms','totalbedrooms','Population','household','medianIncome','medianHouseValue'])
        actual_columns = set(table.columns)
        self.assertEqual(actual_columns, expected_columns)

if __name__ == '__main__':
    unittest.main()
