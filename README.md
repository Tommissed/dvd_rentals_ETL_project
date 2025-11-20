# dvd_rentals_ETL_project

## Objective
The objective of this project is to showcase what we've learned in the last 4 weeks in the 2025-09 Data Engineering Camp.

Over the past 4 weeks, we've learned:

- How to build data integration pipelines (extract load) using data integration tools like Airbyte
- How to use cluster compute engines to transform data i.e. Snowflake and Databricks (Spark)
- How to create DAGs for your transformations using dbt and databricks workflows
- How to create data quality tests for your transformations using dbt tests and great expectations
- How to perform data modeling using techniques such as Dimensional Modeling and One Big Table (OBT)

We created a pipeline using the sample database DVD Rental. We used Airbyte to extract the data into Snowflake and used dbt to handle our transformations and data modeling. We use Tableau/PowerBI as the visualisation layer. 

## Consumers

## Datasets
The ER diagram of the DVD Rental database can be found [here](https://neon.com/postgresqltutorial/printable-postgresql-sample-database-diagram.pdf).

## Solution Architecture

## ELT Process

### Extract/Load
We used Airbyte to extract data from a PostGreSQL database hosted in a docker image. We've set up the Airbyte connection to incrementally extract each table and load into 'raw' schema in a Snowflake database.

### Transform

# Business Process Modeling
