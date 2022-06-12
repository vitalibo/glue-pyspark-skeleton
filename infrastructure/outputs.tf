output "profile" {
  value = var.profile
}

output "region" {
  value = var.region
}

output "environment" {
  value = var.environment
}

output "name" {
  value = var.name
}

output "bucket_name" {
  value = var.bucket_name
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
