
# Data Pipeline With Airflow

## Overview

A music streaming startup, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

In this project, we will create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.


## DataSets

- Log data: s3://udacity-dend/log_data

- Song data: s3://udacity-dend/song_data

## Project Structure

* DAG
  * udac_pipeline_airflow : Load and transform data in Redshift with Airflow

* Operators
  * CreateTableOperator     : create staging tables, fact table and dimensions tables
  * StageToRedshiftOperator : load JSON formatted files from S3 to Amazon Redshift
  * LoadFactOperator        : load fact table from staging table
  * LoadDimensionOperator   : load dimensions tables from staging table
  * DataQualityOperator     : run checks on the tables

* Helper class
  * SqlQueries : SQL transformations   

## Graph view



