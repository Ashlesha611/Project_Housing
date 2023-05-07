import unittest
from pyspark.sql import SparkSession
import os

class TestSparkJobs(unittest.TestCase):

    
    def setUpClass(cls):
        """
        Create a SparkSession and set the path for the Delta table.
        """
       
	builder = ( 
		pyspark.sql.SparkSession.builder.appName("Delta_Table")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   )
         cls.spark = configure_spark_with_delta_pip(builder).appName("unit-tests).getOrCreate()
         cls.delta_table_path = "/FileStore/tables/delta_table"

    def test_create_delta_table(self):
        """
        Test that the Delta table is created with the expected schema.
        """

	  # Create the Delta table

	dt1 = (DeltaTable.create(self.spark).tableName("housing-dataset").addColumn("longitude", dataType="DOUBLE", nullable=True).addColumn("latitude", dataType="DOUBLE",  nullable=True).addColumn("housingMedianAge", dataType="DOUBLE", nullable=True).addColumn("totalRooms", dataType="DOUBLE", nullable=True).addColumn("totalBedrooms", dataType="DOUBLE", nullable=True).addColumn("population", dataType="DOUBLE", nullable=True).addColumn("households", dataType="DOUBLE", nullable=True).addColumn("medianIncome", dataType="DOUBLE", nullable=True).addColumn("medianHouseValue",dataType="DOUBLE".execute())

        
        # Check that the table exists and has the expected schema
        table_exists = self.spark.catalog._jcatalog.tableExists("housing-dataset")
        self.assertTrue(table_exists)

        table = self.spark.table("housing-dataset")
        expected_columns = set(['Longitude','Latitude','housingMedianAge','totalRooms','totalbedrooms','Population','household','medianIncome','medianHouseValue'])
        actual_columns = set(table.columns)
        self.assertEqual(actual_columns, expected_columns)

    def test_create_delta_table_invalid_schema(self):
    	"""
    	Test that an error is raised if an invalid schema is provided for the Delta table.
    	"""
    	with self.assertRaises(Exception) as context:
       		# Create the Delta table with an invalid schema
        	self.spark.sql("CREATE TABLE IF NOT EXISTS housing_dataset USING DELTA " \

    def test_ingest_data_into_delta_table(self):
        """
        Test that data can be ingested into the Delta table and that the table has the expected number of rows.
        """
        # Ingest data into the Delta table
       housing_df = self.spark.read.format("csv").load("housing_data")

       housing_df.write.format("delta").mode("append").save(self.delta_table_path)

        # Check that the table has the expected number of rows
        table = self.spark.table("housing-dataset")
        expected_rows = housing_df.count()
        actual_rows = table.count()
        self.assertEqual(actual_rows, expected_rows)


                      

                      


if __name__ == '__main__':
    unittest.main()
