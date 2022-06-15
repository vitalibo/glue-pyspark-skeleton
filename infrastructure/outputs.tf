output "profile" {
  description = "The AWS profile name"
  value       = var.profile
}

output "region" {
  description = "The AWS region name"
  value       = var.region
}

output "environment" {
  description = "Environment name"
  value       = var.environment
}

output "jobs" {
  description = "List of Glue jobs used for integration tests"
  value       = [
    module.sample_job.job_id
  ]
}

output "data_catalog" {
  description = "Name of the Glue data catalog"
  value       = aws_glue_catalog_database.data_lake.name
}
