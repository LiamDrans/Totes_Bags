resource "aws_s3_bucket" "data_ingestion" {
    bucket_prefix = "totes-data-"

    tags = {
        Name ="DataIngestionBucket"
        Environment = "Extract"
    }
}

resource "aws_s3_bucket" "code_bucket" {
    bucket_prefix = "totes-lambda-code-"

    tags = {
        Name = "LambdaCodeBucket"
        Environment = "Extract"
    }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_ingestion_lifecycle" {
    bucket = aws_s3_bucket.data_ingestion.id

    rule {
        id ="TransitionToInfrequentAccess"
    filter {
        prefix = ""
    }
    transition {
        days = 30
        storage_class = "STANDARD_IA"
    }

    expiration {
        days = 90
    }

    status = "Enabled"
    }
}

# resource "aws_s3_bucket" "terraform_state_file" {
#     bucket_prefix = "terraform-state-file-team-sidley"
# }

