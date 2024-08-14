data "archive_file" "extract_lambda" {
    type        = "zip"
    output_path = "${path.module}/../packages/extract_sample/function.zip"
    source_file = "${path.module}/../src/extract_sample.py"
}

data "archive_file" "transform_lambda" {
    type        = "zip"
    output_path = "${path.module}/../packages/transform_sample/function.zip"
    source_file = "${path.module}/../src/transform_sample.py"
}

data "archive_file" "load_lambda" {
    type        = "zip"
    output_path = "${path.module}/../packages/load_sample/function.zip"
    source_file = "${path.module}/../src/load_sample.py"
}

resource "aws_lambda_function" "task_extract" {
    function_name    = var.extract_lambda
    source_code_hash = data.archive_file.extract_lambda.output_base64sha256
    s3_bucket        = aws_s3_bucket.code_bucket.bucket
    s3_key           = "${var.extract_lambda}/function.zip"
    role             = aws_iam_role.lambda_role.arn
    handler          = "${var.extract_lambda}.lambda_handler"
    runtime          = "python3.12"
    timeout          = var.default_timeout
    log_group_name   = aws_cloudwatch_log_group.logs.name
    # add layers = ....

    depends_on = [aws_s3_object.lambda_code]
    #needs to depend on layer also
}

resource "aws_lambda_function" "task_transform" {
    function_name    = var.transform_lambda
    source_code_hash = data.archive_file.transform_lambda.output_base64sha256
    s3_bucket        = aws_s3_bucket.code_bucket.bucket
    s3_key           = "${var.transform_lambda}/function.zip"
    role             = aws_iam_role.lambda_role.arn
    handler          = "${var.transform_lambda}.lambda_handler"
    runtime          = "python3.12"
    timeout          = var.default_timeout
    log_group_name   = aws_cloudwatch_log_group.logs.name

    depends_on = [aws_s3_object.lambda_code]
}

resource "aws_lambda_function" "task_load" {
    function_name    = var.load_lambda
    source_code_hash = data.archive_file.load_lambda.output_sha256
    s3_bucket        = aws_s3_bucket.code_bucket.bucket
    s3_key           = "${var.load_lambda}/function.zip"
    role             = aws_iam_role.lambda_role.arn
    handler          = "${var.load_lambda}.lambda_handler"
    runtime          = "python3.12"
    timeout          = var.default_timeout
    log_group_name   = aws_cloudwatch_log_group.logs.name

    environment {
        variables = {
        BUCKET_NAME = aws_s3_bucket.data_ingestion.id
        }
    }

    depends_on = [aws_s3_object.lambda_code]
}

resource "aws_s3_object" "lambda_code" {
    for_each = toset([var.extract_lambda, var.transform_lambda, var.load_lambda])
    bucket   = aws_s3_bucket.code_bucket.bucket
    key      = "${each.key}/function.zip"
    source   = "${path.module}/../packages/${each.key}/function.zip"
    etag     = filemd5("${path.module}/../packages/${each.key}/function.zip")
}
