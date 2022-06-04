variable "profile" {
  type        = string
  description = "(Required) Use a specific profile from your credential file"
  default     = "default"
}

variable "region" {
  type        = string
  description = "(Optional) The AWS region to use"
  default     = "eu-west-2"
}

variable "environment" {
  type        = string
  description = "(Required) Environment name"
}

variable "name" {
  type        = string
  description = "(Required) Service name that will be prefixed to resource names"
}

variable "script_home" {
  type        = string
  description = "(Required) Specifies the S3 path to a scripts that executes a job"
}

variable "bucket_name" {
  type        = string
  description = "(Required) S3 bucket name where will be stored data platform artifacts"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "(Optional) A list of additional tags to apply to resources"
}
