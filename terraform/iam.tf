resource "aws_iam_user" "bucket_user" {
  name = "s3-retailitics-accessor"
}

data "aws_iam_policy_document" "bucket_policy_doc" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.my_bucket.arn,
      "${aws_s3_bucket.my_bucket.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "bucket_policy" {
  name        = "s3-access-policy"
  description = "policy allowing access to S3 bucket"
  policy      = data.aws_iam_policy_document.bucket_policy_doc.json
}

resource "aws_iam_user_policy_attachment" "bucket_user_attachment" {
  user       = aws_iam_user.bucket_user.name
  policy_arn = aws_iam_policy.bucket_policy.arn
}