# Project 


#Intall Delta Lake
!pip3 install delta-spark==2.2.0


# Imports

import pyspark
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import *



# Creating Spark Session

builder = (
    pyspark.sql.SparkSession.builder.appName("Delta_Table")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Creating delta table

dt1 = (DeltaTable.create(spark).tableName("housing-dataset").addColumn("longitude", dataType="DOUBLE", nullable=True).addColumn("latitude", dataType="DOUBLE", nullable=True).addColumn("housingMedianAge", dataType="DOUBLE", nullable=True).addColumn("totalRooms", dataType="DOUBLE", nullable=True).addColumn("totalBedrooms", dataType="DOUBLE", nullable=True).addColumn("population", dataType="DOUBLE", nullable=True).addColumn("households", dataType="DOUBLE", nullable=True).addColumn("medianIncome", dataType="DOUBLE", nullable=True).addColumn("medianHouseValue",dataType="DOUBLE".execute())


dt1

spark.table("housing-dataset").show()

# Ingesting data into delta table 

housing_df = spark.read.format("csv").load("housing_data")

housing_df.write.format("delta").mode("overwrite").save("housing-dataset")

# To see data from Delta table 

spark.sql('select * from housing-dataset').show()


