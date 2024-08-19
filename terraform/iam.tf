resource "aws_iam_role" "lambda_role" {
    name_prefix        = "role-totes-lambdas-"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
    statement {

    actions = ["s3:PutObject"]

    resources = [
        "${aws_s3_bucket.data_ingestion.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "cloudwatch_logs_policy_document" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_policy" "cloudwatch_logs_policy" {
  name_prefix = "cloudwatch-logs-policy-totes-lambda-"
  policy      = data.aws_iam_policy_document.cloudwatch_logs_policy_document.json
}

data "aws_iam_policy_document" "secrets_manager_document" {
    statement {

    actions = ["secretsmanager:GetSecretValue",
				"secretsmanager:DescribeSecret"]

    resources = [
        "arn:aws:secretsmanager:eu-west-2:730335560557:secret:totesys_db-*",
    ]
  }
}



resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-totes-lambda-"
    policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_logs_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_manager_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.secrets_manager_policy.arn
}




