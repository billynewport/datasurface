# Yellow Data Platform

This is a builtin Yellow but powerful DataPlatform that can be used with DataSurface. It uses a Postgres instance to store its data. The data consists of staging data and merge tables. Consumers directly query the merge tables through Workspace specific views. Workspaces should use the LogicalDataContainer as their DataContainer.

## Staging Data

Staging data is captured from a source system and stored in a table. The data stays there and is processed by a job to augment the data in the merge table. For example, staging data might be grouped by a batch id and each record might have a enum column indicating whether the record represents an insert, update or a delete. The job will then process the data and insert it into the merge table. Staging data tables are used by the DataPlatform and not typically used by consumers of Workspace data. They are internal tables.

## Merge Tables

These represent usable data which can be read by consumers. They are a post processed version of the staging data. When a Workspace uses a data set then this will cause the DataPlatform to create a single staging table and a single merge table for it. A view will also be created named after the dataset and the Workspace name. Consumers query the data using the view. Only the view will have permissions allowing consumers to read the data. The merge table and the staging table are internal tables and are not visible to consumers.

For a live only MERGE, the MERGE table will have a single record for each key in the source dataset. The batches of records captured in the staging table will be merged in to this table to remove old deleted records, insert new records and update existing records.

For a milestoned MERGE, the MERGE table will contain two extra columns per record, a batch_in and a batch_out column. The batch_in column is the batch id when this record was either inserted or updated. The batch_out column is the batch when this record was updated or deleted. The batch_out column will be -1 for records which have not been modifed since they were inserted or updated last, i.e., they are still current.

## Why use Postgres?

I'm using postgres because for the vast majority of use cases, it's good enough for the job. It's also a SQL based platform. SQL based is important because SQL is standard. We can imagine replacing postgres with a columnar storage database such as Snowflake or Athena or Azure SQL or similar without much work. These columnar storage databases are getting better everyday and all are focused on efficiently executing SQL. Thus, it makes sense to bet on using SQL rather than Spark as the SQL execution is getting better everyday, portable, and lets face it, after 40 years of SQL, it's not going anyway anytime soon. A user could use a postgres database hosted on AWS for example. AWS features servers which can scale up to 192 vCores with 768GB of RAM. I'm not saying postgres can run on 192 vCores, I'm sure there are critical section bottlenecks but a m7i.8xlarge box with 32 vCores and 128GB of RAM would be a very large postgres instance and be capable of a surprising amount of work. Remember, the fastest distributed database is a single node database large enough to execute the workload. Distribution has a cost and a single node system will outperform a distributed system up to a certain point.There are also managed AWS RDS instances which could be used.

So, we will start YellowDataPlatform with Postgres and Airflow as the job scheduler. This is a simple system and with Postgres servers with terabytes of storage, lots of memory and lots of cores, it can scale well. It also supports indexes which makes performance tuning relatively easy compared with native columnar storage databases.

## Columnar future

Once we have a working system then we can look at refactoring it in to a portable layer which is common across SQL based engines and then implement a (hopefully) thin layer for each SQL data container: Postgres, Athena, Azure SQL, Snowflake

## Container Orchestration

The YellowDataPlatform will be designed alongside the Docker Swarm RenderEngine. Once this is working then we can look at other container orchestration engines. An obvious next step is Kubernetes.
