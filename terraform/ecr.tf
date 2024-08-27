resource "aws_ecr_repository" "lambda_repo" {
  name = "lambda_layer_repo"
}

resource "null_resource" "docker_build_push" {
  triggers = {
    dockerfile = filemd5("${path.module}/../Dockerfile")
    pyproject = filemd5("${path.module}/../pyproject.toml")
  }

    provisioner "local-exec" {
    command = <<-EOT
        docker build -t ${aws_ecr_repository.lambda_repo.repository_url}:latest ${path.module}/..
        aws ecr get-login-password --region "eu-west-2" | docker login --username AWS --password-stdin ${aws_ecr_repository.lambda_repo.repository_url}
        docker push ${aws_ecr_repository.lambda_repo.repository_url}:latest
    EOT
    }
}