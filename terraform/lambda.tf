locals {
  combined_hash_code = "${filemd5("${path.module}/../packages/extract/function.zip")}-${filemd5("${path.module}/../pyproject.toml")}"
}

data "archive_file" "extract" {
    type        = "zip"
    output_path = "${path.module}/../packages/extract/function.zip"
    source_dir  = "${path.module}/../src/extract"
}

data "archive_file" "transform" {
    type        = "zip"
    output_path = "${path.module}/../packages/transform/function.zip"    
    source_dir  = "${path.module}/../src/transform"
}

data "archive_file" "load" {
    type        = "zip"
    output_path = "${path.module}/../packages/load/function.zip"
    source_dir  = "${path.module}/../src/load"
}

resource "aws_lambda_function" "task_extract" {
    function_name    = var.extract_lambda
    source_code_hash = local.combined_hash_code
    s3_bucket        = aws_s3_bucket.code_bucket.bucket
    s3_key           = "${var.extract_lambda}/function.zip"
    role             = aws_iam_role.lambda_role.arn
    handler          = "app.extract.lambda_handler"
    runtime          = "python3.12"
    timeout          = var.default_timeout
    layers           = ["${aws_lambda_layer_version.dependencies.arn}"]
    memory_size      = 256
    logging_config {
        log_format   = "Text"
        log_group    = aws_cloudwatch_log_group.logs.name
    }
    environment {
      variables = {
        TZ="Europe/London"
    }
    }

    depends_on = [aws_s3_object.lambda_code]
}

resource "aws_lambda_function" "task_transform" {
    function_name    = var.transform_lambda
    source_code_hash = data.archive_file.transform.output_base64sha256
    s3_bucket        = aws_s3_bucket.code_bucket.bucket
    s3_key           = "${var.transform_lambda}/function.zip"
    role             = aws_iam_role.lambda_role.arn
    handler          = "app.${var.transform_lambda}.lambda_handler"
    runtime          = "python3.12"
    timeout          = var.default_timeout
    layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:13"]
    memory_size = 256
    logging_config {
        log_format   = "Text"
        log_group    = aws_cloudwatch_log_group.logs.name
    }

    depends_on = [aws_s3_object.lambda_code]
     environment {
      variables = {
        TZ="Europe/London"
    }
    }
}

resource "aws_lambda_function" "task_load" {
    function_name    = var.load_lambda
    source_code_hash = data.archive_file.load.output_sha256
    s3_bucket        = aws_s3_bucket.code_bucket.bucket
    s3_key           = "${var.load_lambda}/function.zip"
    role             = aws_iam_role.lambda_role.arn
    handler          = "${var.load_lambda}.lambda_handler"
    runtime          = "python3.12"
    timeout          = var.default_timeout
    memory_size = 256
    layers         = ["${aws_lambda_layer_version.dependencies.arn}"]
    logging_config {
        log_format   = "Text"
        log_group    = aws_cloudwatch_log_group.logs.name
    }
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
    # source_hash = data.archive_file.for_each[each.key].output_base64sha256  
       
}
