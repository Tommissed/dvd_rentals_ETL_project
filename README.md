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

## Dataset
We are using the sample PostGresql DVD_Rentals database. The ER diagram can be found [here](https://neon.com/postgresqltutorial/printable-postgresql-sample-database-diagram.pdf).

## Solution Architecture

## ELT Process

### Extract/Load
We used Airbyte to extract data from a PostGreSQL database hosted in a docker image. We've set up the Airbyte connection to incrementally extract each table and load into 'raw' schema in a Snowflake database. The raw schema serves as our Bronze layer.
<br/><br/>
![Airbyte Connection](https://github.com/Tommissed/dvd_rentals_ETL_project/blob/main/images/Airbyte%20Connection.png?raw=true)


### Transform
We used dbt to build foundational staging models (Silver layer), where raw source tables are standardized through basic transformations such as data type casting.
<br/><br/> 
From there, we developed curated dimension and fact models (Gold layer) that serve as the primary analytical data sets for end users.

## Data Visualizations
