provider "aws" {
  region  = var.region
  profile = var.profile
}

locals {
  tags = merge({
    Terraform   = "true"
    Environment = var.environment
  }, var.tags)
}

data "template_file" "config" {
  template = join("\n---\n", [for name in fileset("${path.module}/../profile/", "*") : file("${path.module}/../profile/${name}")])
  vars     = {
    environment = var.environment
    name        = var.name
    bucket_name = var.bucket_name
  }
}

resource "null_resource" "code" {
  triggers = {
    driver  = "${var.script_home}/driver.py"
    package = "${var.script_home}/glue_pyspark_skeleton-1.0.0-py3-none-any.whl"
    config  = "${var.script_home}/application.yaml"
    hash    = md5(data.template_file.config.rendered)
  }

  provisioner "local-exec" {
    command = <<EOT
set -e

(cd .. && make clean build)
echo "${data.template_file.config.rendered}" > ${path.module}/../dist/application.yaml

aws s3 cp ${path.module}/../src/dp/driver.py ${self.triggers.driver} --profile=${var.profile}
aws s3 cp ${path.module}/../dist/glue_pyspark_skeleton-1.0.0-py3-none-any.whl ${self.triggers.package} --profile=${var.profile}
aws s3 cp ${path.module}/../dist/application.yaml ${self.triggers.config} --profile=${var.profile}

(cd ..  && make clean)
EOT
  }
}

module "sample_job" {
  source            = "vitalibo/glue-job/aws"
  environment       = var.environment
  name              = "${var.name}-sample-job"
  role_arn          = aws_iam_role.sample_job_role.arn
  script_location   = null_resource.code.triggers.driver
  extra_py_files    = [null_resource.code.triggers.package]
  extra_files       = [null_resource.code.triggers.config]
  extra_jars        = ["https://s3.us-west-2.amazonaws.com/crawler-public/json/serde/json-serde.jar"]
  job_language      = "python"
  timeout           = 10
  number_of_workers = 2
  tags              = local.tags
}

resource "aws_iam_role" "sample_job_role" {
  name = "${var.environment}-${var.name}-sample-job-role"
  tags = local.tags

  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueAssumeRole"
        Effect = "Allow"
        Action = [
          "sts:AssumeRole"
        ]
        Principal = {
          "Service" = "glue.amazonaws.com"
        }
      }
    ]
  })

  inline_policy {
    name   = "Runtime"
    policy = jsonencode({
      Version   = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = [
            "*"
          ]
          Resource = "*"
        }
      ]
    })
  }

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]
}

terraform {
  required_version = ">= 1.1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.17.0"
    }
  }

  backend "s3" {
  }
}
