# Getting a local kafka connect zero platform setup working

## Install kafka connect and various pieces

The docker-compose.yaml file will install a local complete kafka connect setup. It's taken from the official github repository.

## Install minio server locally

We need minio as an s3 compatible storage solution to store staging files captured by debezium as well as the MERGED corpus files generated with live records which we can query with trino.

## Install mysql server locally

We need a source database to capture changes from. We'll use mysql for this.

## Install trino

We need trino to query the iceberg tables. We'll install trino locally.

## Using terraform to configure a zero prototype pipeline

The terraform file prototype_zero.tf will provision kafka topics and debezium connectors for a prototype pipeline. This pipeline will capture changes from a mysql database and store the deltas in a minio bucket. This uses 2 providers, kafka and kafka connect, both from mongey. Kafka is available in the official terraform registry. Kafka connect must be downloaded and installed locally in the terraform plugins folder. We will get the kafka-connect provider from the mongey github repository [https://github.com/Mongey/terraform-provider-kafka]. We will download the 0.8.2 release and install it in the terraform plugins folder in the following folder: mongey/kafka-connect/0.8.2

The zip file needs to be extracted and the binary file placed in that folder.
