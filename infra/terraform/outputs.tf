output "lakehouse_bucket" {
  description = "Bucket del lakehouse. Lo lee scripts/aws_deploy.ps1 para subir los artefactos."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "glue_jobs" {
  description = "Nombres de los tres jobs de Glue, en el orden en que corren."
  value = [
    aws_glue_job.ingest_produccion_pozo.name,
    aws_glue_job.bronze_produccion_pozo.name,
    aws_glue_job.silver_produccion_pozo.name,
  ]
}

output "state_machine_arn" {
  description = "ARN de la máquina de estados que encadena los tres jobs."
  value       = aws_sfn_state_machine.produccion_pozo_mensual.arn
}

output "athena_workgroup" {
  description = "Workgroup de Athena para consultar bronze y silver."
  value       = aws_athena_workgroup.lakehouse.name
}
