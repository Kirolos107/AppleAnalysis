# AppleAnalysis

AppleAnalysis is a data engineering project that leverages Databricks and PySpark to create multiple ETL pipelines. The project utilizes sources like CSV, Parquet, and Delta Table, implements the Factory Pattern for the reader class, and applies business transformation logic using PySpark DataFrame API and Spark SQL.

## Project Description

In this project, we used Databricks to create multiple ETL pipelines using the Python API of Apache Spark, i.e., PySpark. The project handles multiple data sources and uses the Factory Pattern to create the reader class, a commonly used low-level design in data engineering pipelines.

## Technologies Used

- Databricks
- PySpark
- Apache Spark
- DataFrame API
- Spark SQL
- DataLake
- CSV, Parquet, Delta Table

## Steps

1. **Create Databricks Environment**
   - Set up a Databricks account and workspace.
2. **Create Cluster**
   - Set up a cluster in Databricks to run your PySpark code.
3. **Add Source Files to DBFS (Databricks File System)**
   - Upload the source files (CSV, Parquet, Delta Table) to DBFS.
4. **Create Notebook for Loader Factory and Reader Factory**
   - Develop helper functions and implement the Factory Pattern for creating reader classes.
5. **Create Notebook for Extract, Transform, Load Classes**
   - Structure the ETL pipeline by creating classes for extraction, transformation, and loading.
6. **Create ETL Pipelines**
   - **First Pipeline**: Identify customers who bought AirPods first, then an iPhone, and load the results to DBFS.
   - **Second Pipeline**: Identify customers who bought AirPods first, then an iPhone, and load the results to DBFS with partitioning by location.
   - **Third Pipeline**: Calculate the delay between buying an iPhone and AirPods and load the results to DBFS.

## Installation

To run this project, follow these steps:

1. Set up a [Databricks account](https://databricks.com/).
2. Create a cluster in your Databricks workspace.
3. Upload the source files (CSV, Parquet, Delta Table) to DBFS.
4. Create notebooks for the loader factory, reader factory, and ETL classes.
5. Execute the notebooks in Databricks to run the ETL pipelines.

## Notebooks


1. `Apple_Analysis` - Defines the ETL pipeline structure and processes the data.
2. `Reader_Factory` - Implements the Factory Pattern for creating reader classes.
3. `Loader_Factory` - Contains helper functions for the loading process.
4. `Extractor` - Contains helper functions for the extracting process.
5. `Transformer` - Contains helper functions for the loading process.
6. `Loader` - Contains helper functions for the loading process.

## Pipelines

- **Pipeline 1**: Customers who bought AirPods first, then an iPhone.
- **Pipeline 2**: Customers who bought AirPods first, then an iPhone, with partitioning by location.
- **Pipeline 3**: Delay between buying an iPhone and AirPods.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

