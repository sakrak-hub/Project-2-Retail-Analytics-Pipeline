resource "aws_iam_role" "kafka_ec2_s3_role" {
  name = "retailitics-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "kafka_s3_read_write" {
  name = "retailitics-s3-read-write"
  role = aws_iam_role.kafka_ec2_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketVersions" 
        ]
        Resource = "arn:aws:s3:::my-retail-2026-analytics-5805"
      },
      {
        Sid    = "AllowRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = "arn:aws:s3:::my-retail-2026-analytics-5805/execute/*"
      },
      {
        Sid    = "AllowReadParquet"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = "arn:aws:s3:::my-retail-2026-analytics-5805/retail_data/*.parquet"
      },
      {
        Sid    = "AllowReadWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:DeleteObject" 
        ]
        Resource = "arn:aws:s3:::my-retail-2026-analytics-5805/retail_data/transactions/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "kafka_ec2_profile" {
  name = "retailitics-kafka-ec2-profile"
  role = aws_iam_role.kafka_ec2_s3_role.name
}
