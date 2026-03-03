variable "region" {
  description = "AWS region for resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project identifier used for resource naming."
  type        = string
  default     = "mini-cloud-data-platform"
}

variable "env" {
  description = "Deployment environment (dev by default)."
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Additional tags to add to all resources."
  type        = map(string)
  default     = {}
}

variable "bucket_force_destroy" {
  description = "Allow Terraform to delete non-empty bucket (use false in most cases)."
  type        = bool
  default     = false
}

variable "create_glue_catalog" {
  description = "If true, create a simple Glue Data Catalog database."
  type        = bool
  default     = false
}
