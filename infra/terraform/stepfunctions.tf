# La máquina de estados es el equivalente en AWS del DAG produccion_pozo_mensual de Airflow:
# los mismos tres pasos, en el mismo orden, y si uno falla no arranca el siguiente.
#
# `startJobRun.sync` espera a que el job termine y falla si el job falla.
#
# Cada paso toma sus overrides del input de la ejecución (JSONata): con input `{}` cada job
# corre con sus argumentos por defecto, y con
#   {"ingesta": {"--only": "^Padr"}, "silver": {"--contract": "pozo_primera_produccion"}}
# se acota la corrida sin tocar la definición del job.

locals {
  # `$states.context.Execution.Input` y no `$states.input`: el input de un paso es la salida
  # del paso anterior (la corrida de Glue), no el input de la ejecución.
  # El `? :` es obligatorio: una expresión JSONata que no devuelve nada corta la ejecución
  # con QueryEvaluationError en vez de omitir el campo.
  overrides = { for paso in ["ingesta", "bronze", "silver"] :
    paso => "{% $exists($states.context.Execution.Input.${paso}) ? $states.context.Execution.Input.${paso} : {} %}"
  }

  definicion_produccion_pozo_mensual = {
    Comment       = "produccion_pozo: landing -> bronze -> silver"
    QueryLanguage = "JSONata"
    StartAt       = "ingesta"
    States = {
      ingesta = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = {
          JobName   = aws_glue_job.ingest_produccion_pozo.name
          Arguments = local.overrides["ingesta"]
        }
        Next = "bronze"
      }
      bronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = {
          JobName   = aws_glue_job.bronze_produccion_pozo.name
          Arguments = local.overrides["bronze"]
        }
        Next = "silver"
      }
      silver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = {
          JobName   = aws_glue_job.silver_produccion_pozo.name
          Arguments = local.overrides["silver"]
        }
        End = true
      }
    }
  }
}

resource "aws_sfn_state_machine" "produccion_pozo_mensual" {
  name       = "produccion_pozo_mensual"
  role_arn   = aws_iam_role.step_functions.arn
  definition = jsonencode(local.definicion_produccion_pozo_mensual)
}

# Mensual, igual que el DAG. Nace deshabilitado: el entorno no tiene que quedar corriendo
# solo. Se habilita con `terraform apply -var enable_schedule=true`.
resource "aws_scheduler_schedule" "produccion_pozo_mensual" {
  name                         = "produccion-pozo-mensual"
  state                        = var.enable_schedule ? "ENABLED" : "DISABLED"
  schedule_expression          = "cron(0 6 1 * ? *)"
  schedule_expression_timezone = "America/Argentina/Buenos_Aires"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.produccion_pozo_mensual.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})
  }
}
