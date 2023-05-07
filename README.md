# Project_Housing

This project creates a Delta table in Pyspark and ingests data into it. It includes two Spark jobs:

a) Creating a job that creates a Delta table in Spark.
     	Table name = housing-dataset
 
b) Ingesting data into Pyspark-based Delta Table
     The sink will be the delta table.


The project also includes unit tests and integration tests to verify the functionality of the Spark jobs.

## Getting Started
To use this project, you'll need access to a Pyspark cluster with the appropriate dependencies installed.

Prerequisites
Before running the Spark jobs, you'll need to install the necessary dependencies.
 
## Running the Spark Jobs
To run the Spark jobs, you can use a notebook in a Spark cluster, or you can submit the jobs as standalone applications.

Running the jobs using a Pycharm
To run the Spark jobs using Pycharm, follow these steps:

1. Open pycharm which has spark as a processing framework. 
2. Write or copy the codes and modify the parameters as necessary (e.g., the paths to the source data and the Delta table).
3. Run the codes to execute the Spark jobs.

Running the jobs as standalone applications
To run the Spark jobs as standalone applications, first copy code into .py file then you can use the spark-submit command:

```
spark-submit file.py
```
## Running the Unit Tests
To run the unit tests, you'll need to install the pytest package:

```
pip install pytest
```
Then, you can run the tests using the pytest command:

```
pytest unit_test.py
```
## Running the Integration Tests
To run the integration tests, you'll need to have a Spark cluster with the appropriate dependencies installed.

You can run the integration tests using the pytest command:

```
pytest test_integration.py
```
