# Project description
As an evolution of [Project 4](https://github.com/hedcler/udacity-dataengineer-project4), a music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

As their data engineer, I was tasked tasked with building an ETL pipeline that extracts their data from S3, processes them using Airflow, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Step 1 - Configure the project dependencies
It's good to start with airflow on docker to avoid too much environment challenges. 
Install the [Docker](https://docs.docker.com/get-docker/) and [Docker-Compose](https://docs.docker.com/compose/install/) on your host.

### Step 2 - Run Apache Airflow on Docker
With Docker installed and running, and Docker-Composer installed, run the follow command on your terminal:

```sh
$ docker-compose up -d
```

This command will start the Apache Airflow, and it will be accessible on http://localhost:8080, to login use the follow credentials:

```
username: ayrflow
password: ayrflow
```

