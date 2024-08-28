resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = <<-EOT
    cd ${path.module}/..
    poetry update
    poetry export -f requirements.txt --output requirements.txt --without-hashes
    poetry run pip install -r requirements.txt -t dependencies/python
    EOT
  }
  triggers = {
    dependencies = filemd5("${path.module}/../pyproject.toml")
  }
}

data "archive_file" "layer_code" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/layer.zip"
  source_dir  = "${path.module}/../dependencies"
  depends_on = [ null_resource.create_dependencies ]
}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name = "requests_dependencies_layer"
  source_code_hash = local.combined_hash_code
  s3_bucket  = aws_s3_object.lambda_layer.bucket
  s3_key     = aws_s3_object.lambda_layer.key
  depends_on = [ aws_s3_object.lambda_layer ]
}
