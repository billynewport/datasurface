variable "replication_task_id" {
  description = "The ID of the replication task"
  type        = string
}

variable "table_names" {
  description = "List of table names for the replication task"
  type        = list(string)
}

variable "source_endpoint_arn" {
  description = "The ARN of the source endpoint"
  type        = string
}

variable "target_endpoint_arn" {
  description = "The ARN of the target endpoint"
  type        = string
}

variable "replication_instance_arn" {
  description = "The ARN of the replication instance"
  type        = string
}

resource "aws_dms_replication_task" "task" {
  replication_task_id      = var.replication_task_id
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = var.replication_instance_arn
  source_endpoint_arn      = var.source_endpoint_arn
  target_endpoint_arn      = var.target_endpoint_arn

  dynamic "table_mappings" {
    for_each = var.table_names
    content {
      rules = [
        {
          "rule-type"    = "selection"
          "rule-id"      = "${table_mappings.key + 1}"
          "rule-name"    = "${table_mappings.key + 1}"
          "object-locator" = {
            "schema-name" = "%"
            "table-name"  = table_mappings.value
          }
          "rule-action"  = "include"
        }
      ]
    }
  }
}