terraform {
    required_providers {
        aws = {
        source = "hashicorp/aws"
        version = "~> 5.0"
        }
    }
}

provider "aws" {
    region = "eu-west-2"
}

# terraform {
#     backend "s3" {
#         bucket = "terraform-state-file-sidley"
#         key = "terraform.tfstate"
#         region = "eu-west-2"
#         encrypt = true
#     }
# }

