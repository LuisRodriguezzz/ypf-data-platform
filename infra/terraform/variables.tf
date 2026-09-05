variable "project" {
  description = "Nombre del proyecto; se aplica como tag a todos los recursos."
  type        = string
  default     = "ypf-data-platform"
}

variable "region" {
  description = "Región de AWS."
  type        = string
  default     = "us-east-1"
}

variable "bucket_suffix" {
  description = "Sufijo opcional del bucket, por si hace falta un segundo entorno en la misma cuenta."
  type        = string
  default     = ""
}

variable "wheel_name" {
  description = "Nombre del wheel que `scripts/aws_deploy.ps1` sube a artifacts/ (viene de la versión en pyproject.toml)."
  type        = string
  default     = "ypf_data_platform-0.1.0-py3-none-any.whl"
}

variable "postgres_dsn_ssm_parameter" {
  description = "Parámetro SecureString con la cadena de conexión a Neon. Se crea a mano, fuera de Terraform: es un secreto."
  type        = string
  default     = "/ypf-lakehouse/postgres_dsn"
}

variable "enable_schedule" {
  description = "Deja el schedule mensual habilitado. Falso por defecto: nada corriendo si nadie lo pide."
  type        = bool
  default     = false
}
