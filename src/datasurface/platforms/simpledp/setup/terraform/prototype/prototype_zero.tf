terraform {
  required_providers {
    kafka = {
      source = "mongey/kafka"
      version = "0.8.2"
    }

    kafka-connect = {
      source = "mongey/kafka-connect"
      version = "0.4.1"
    }
  }
}

provider "kafka-connect" {
  url = "http://localhost:8083"
}

provider "kafka" {
  bootstrap_servers = ["localhost:9092"]
}

resource "kafka_topic" "schema_changes_inventory" {
  name               = "schema-changes.inventory"
  replication_factor = 1
  partitions         = 3
  config = {
    "cleanup.policy" = "delete"
  }
}

resource "kafka-connect_connector" "debezium_connector" {
  name = "debezium-connector"
  config = {
    "connector.class" = "io.debezium.connector.mysql.MySqlConnector"
    "tasks.max"       = "1"
    "database.hostname" = var.database_hostname
    "database.port"     = "3306"
    "database.user"     = var.database_user
    "database.password" = var.database_password
    "database.server.id" = "184054"
    "database.server.name" = "dbserver1"
    "table.include.list" = "inventory.customers"
    "database.history.kafka.bootstrap.servers" = "localhost:9092"
    "database.history.kafka.topic" = kafka_topic.schema_changes_inventory.name
  }
}

variable "database_hostname" {
  description = "Database Hostname"
  type        = string
}

variable "database_user" {
  description = "Database User"
  type        = string
}

variable "database_password" {
  description = "Database Password"
  type        = string
}

