resource "random_string" "bucket_suffix" {
  length  = 6
  upper   = false
  special = false
}

resource "aws_s3_bucket" "data_lake" {
  bucket        = "${local.base_name}-${random_string.bucket_suffix.result}"
  force_destroy = var.bucket_force_destroy
  tags          = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "bronze/"
  content = ""
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "gold/"
  content = ""
}

resource "aws_iam_user" "pipeline_user" {
  name = "${local.base_name}-pipeline"
  tags = local.common_tags
}

data "aws_iam_policy_document" "pipeline_access" {
  statement {
    sid    = "AllowListDataLakePrefixes"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]
    resources = [aws_s3_bucket.data_lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/*", "silver/*", "gold/*"]
    }
  }

  statement {
    sid    = "AllowReadWriteObjectsInDataLake"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.data_lake.arn}/bronze/*",
      "${aws_s3_bucket.data_lake.arn}/silver/*",
      "${aws_s3_bucket.data_lake.arn}/gold/*"
    ]
  }
}

resource "aws_iam_policy" "pipeline_access" {
  name        = "${local.base_name}-pipeline-s3-access"
  description = "Least-privilege S3 access for bronze/silver/gold prefixes."
  policy      = data.aws_iam_policy_document.pipeline_access.json
  tags        = local.common_tags
}

resource "aws_iam_user_policy_attachment" "pipeline_access" {
  user       = aws_iam_user.pipeline_user.name
  policy_arn = aws_iam_policy.pipeline_access.arn
}

resource "aws_glue_catalog_database" "analytics" {
  count = var.create_glue_catalog ? 1 : 0
  name  = replace("${var.project_name}_${var.env}_analytics", "-", "_")
}
