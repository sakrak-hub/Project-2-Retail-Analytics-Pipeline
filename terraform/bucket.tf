resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-retail-2026-analytics-5805"

  tags = {
    Name        = "MyS3Bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_public_access_block" "bucket_public_access_block" {
  bucket = aws_s3_bucket.my_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "retailitics_versioning" {
  bucket = aws_s3_bucket.my_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "version_lifecycle" {
  bucket = aws_s3_bucket.my_bucket.id

  # Rule 1: Clean up old versions of transaction files
  rule {
    id     = "expire-old-transaction-versions"
    status = "Enabled"

    # Apply only to transaction files
    filter {
      prefix = "retail_data/transactions/"
    }

    # Delete non-current versions after 30 days
    noncurrent_version_expiration {
      noncurrent_days = 2
    }
  }

  # Rule 2: Clean up incomplete multipart uploads
  rule {
    id     = "cleanup-incomplete-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }

  rule {
    id     = "expire-old-masterdata-versions"
    status = "Enabled"

    filter {
      prefix = "retail_data/*.parquet"
    }

    noncurrent_version_expiration {
      noncurrent_days = 5
    }
  }
}