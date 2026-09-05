terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State local a propósito: el entorno es efímero (se crea, se demuestra y se destruye) y
  # lo maneja una sola persona desde una sola máquina. Un backend remoto en S3 pediría un
  # bucket y una tabla de locks que sobrevivirían al `destroy` y costarían plata para nada.
  # `*.tfstate` ya está en el .gitignore del repo.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project = var.project
    }
  }
}
