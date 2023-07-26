# nyc-taxi | Event-Driven architecture

<img src="./images/architecture.png" alt="Image Description" width="400" height="300">


# Overview

To build the event-driven pipeline within the AWS infrastructures.

- Data
    - I found the dataset through the Registry of Open Data on AWS.
        - AWS registry exists to help people discover and share datasets that are available via AWS resources
    - I chose New York City Taxi and Limousine Commission (TLC) Trip Record Data because it offers data from 2009 to 2023. It is well documented and is provided in a parquet format.
    - The data is offered in a parquet format
    
    
- S3
    - the folder structure
        - nyc-taxi-project
            - data
                - ingest
                - processed
            - logs
            - python
    - folder structures
        - `data`
            - the data folder contains ingest and processed folder.
            - `ingest` folder has nyc taxi parquet files
            - `processed` folder has transformed data from spark jobs.
        - `log`
            - any log information will be provided from EMR cluster after a job is submitted
        - `python`
            - pyspark code is stored in this folder.
        
- Lambda
    - Triggers when ingestion is done from `nyc-taxi-project/data/ingest`
    - calls the Airflow API and passes the bucket name and key for the data ingestion
- Airflow
    - a dockerized container in EC2
    - receives the bucket name and the key from Lambda and calls the EMROperator by using spark-submit
- EMR
    - Runs the spark job and performs an ETL with the given file
- Glue
    - runs the crawler to update the catalog
- Superset with Athena
    - a dockerized container in EC2
    - visualize the dataset with Athena
