output "lakehouse_bucket" {
  description = "Bucket del lakehouse. Lo lee scripts/aws_deploy.ps1 para subir los artefactos."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "glue_jobs" {
  description = "Nombres de los jobs de Glue, en el orden en que corren."
  value = [
    aws_glue_job.ingest_landing.name,
    aws_glue_job.bronze_load.name,
    aws_glue_job.bronze_reservas.name,
    aws_glue_job.silver_load.name,
    aws_glue_job.gold_dbt.name,
  ]
}

output "state_machine_arns" {
  description = "ARN de la máquina de estados de cada pipeline, por nombre."
  value       = { for nombre, maquina in aws_sfn_state_machine.pipeline : nombre => maquina.arn }
}

output "athena_workgroup" {
  description = "Workgroup de Athena para consultar bronze y silver."
  value       = aws_athena_workgroup.lakehouse.name
}
