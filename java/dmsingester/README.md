# AWS DMS Spark Ingestor

This is a spark job which reads data from the s3 bucket where an AWS DMS task writes the full load and change events and writes that data to an iceberg table in a different bucket.

## Full load

The job will first read the full load data and create the iceberg table with those records.

## Incremental Load

Once DMS finishes the full load then it starts writing delta change events in to a different s3 bucket. The job will read these change events and convert them to upsert/delete records against the iceberg table.

## Packaging

The job will be packaged as a Docker image. This gets all the dependencies in one place and makes it easy to run the job in any environment.
