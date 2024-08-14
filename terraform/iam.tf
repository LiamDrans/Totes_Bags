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

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-totes-lambda-"
    policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}




