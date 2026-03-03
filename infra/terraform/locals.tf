locals {
  base_name = lower(replace("${var.project_name}-${var.env}", "_", "-"))
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.env
      ManagedBy   = "terraform"
    },
    var.tags
  )
}
