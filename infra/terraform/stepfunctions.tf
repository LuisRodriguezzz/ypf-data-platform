# Una máquina de estados por DAG de Airflow: los mismos tres pasos, en el mismo orden, y si
# uno falla no arranca el siguiente. Los tres jobs de Glue son genéricos y se reutilizan; lo
# único que cambia entre pipelines es el dataset, el contrato y el cron.
#
# `startJobRun.sync` espera a que el job termine y falla si el job falla.
#
# Cada paso mezcla sus argumentos fijos con los que traiga el input de la ejecución (JSONata):
# con input `{}` corre el pipeline completo, y con
#   {"ingesta": {"--only": "^Padr"}, "silver": {"--contract": "pozo_primera_produccion"}}
# se acota la corrida sin tocar la definición.

locals {
  pipelines = {
    produccion_pozo_mensual = {
      dataset  = "produccion_pozo"
      contract = "produccion_pozo"
      # Mensual, el día 1 a las 6, igual que el DAG.
      cron = "cron(0 6 1 * ? *)"
    }
    fractura_diaria = {
      dataset  = "fractura"
      contract = "fractura"
      # Diario a las 7: el portal republica el CSV de fractura todos los días.
      cron = "cron(0 7 * * ? *)"
    }
  }

  # Argumentos fijos de cada paso, por pipeline.
  fijos = { for nombre, pipeline in local.pipelines : nombre => {
    ingesta = { "--dataset" = pipeline.dataset }
    bronze  = { "--dataset" = pipeline.dataset }
    silver  = { "--contract" = pipeline.contract }
  } }

  # `$states.context.Execution.Input` y no `$states.input`: el input de un paso es la salida
  # del paso anterior (la corrida de Glue), no el input de la ejecución.
  # El `? :` es obligatorio: una expresión JSONata que no devuelve nada corta la ejecución
  # con QueryEvaluationError en vez de omitir el campo.
  argumentos = { for nombre, pasos in local.fijos : nombre => { for paso, fijos in pasos :
    paso => "{% $merge([${jsonencode(fijos)}, $exists($states.context.Execution.Input.${paso}) ? $states.context.Execution.Input.${paso} : {}]) %}"
  } }

  definiciones = { for nombre, pipeline in local.pipelines : nombre => {
    Comment       = "${pipeline.dataset}: landing -> bronze -> silver"
    QueryLanguage = "JSONata"
    StartAt       = "ingesta"
    States = {
      ingesta = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = {
          JobName   = aws_glue_job.ingest_landing.name
          Arguments = local.argumentos[nombre]["ingesta"]
        }
        Next = "bronze"
      }
      bronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = {
          JobName   = aws_glue_job.bronze_load.name
          Arguments = local.argumentos[nombre]["bronze"]
        }
        Next = "silver"
      }
      silver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Arguments = {
          JobName   = aws_glue_job.silver_load.name
          Arguments = local.argumentos[nombre]["silver"]
        }
        End = true
      }
    }
  } }
}

resource "aws_sfn_state_machine" "pipeline" {
  for_each = local.definiciones

  name       = each.key
  role_arn   = aws_iam_role.step_functions.arn
  definition = jsonencode(each.value)
}

# Los schedules nacen deshabilitados: el entorno no tiene que quedar corriendo solo. Se
# habilitan con `terraform apply -var enable_schedule=true`.
resource "aws_scheduler_schedule" "pipeline" {
  for_each = local.pipelines

  name                         = replace(each.key, "_", "-")
  state                        = var.enable_schedule ? "ENABLED" : "DISABLED"
  schedule_expression          = each.value.cron
  schedule_expression_timezone = "America/Argentina/Buenos_Aires"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline[each.key].arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})
  }
}
