# Tres roles, uno por servicio que ejecuta algo: los jobs de Glue, la máquina de estados y
# el scheduler. Cada uno con el permiso mínimo para su tarea.

data "aws_iam_policy_document" "asume" {
  for_each = toset(["glue.amazonaws.com", "states.amazonaws.com", "scheduler.amazonaws.com"])

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = [each.key]
    }
  }
}

# --- rol de los jobs de Glue ------------------------------------------------

resource "aws_iam_role" "glue_job" {
  name               = "${var.project}-glue-job"
  description        = "Rol que asumen los tres jobs de Glue del pipeline."
  assume_role_policy = data.aws_iam_policy_document.asume["glue.amazonaws.com"].json
}

# Permisos base de Glue (catálogo, CloudWatch Logs, buckets aws-glue-*).
resource "aws_iam_role_policy_attachment" "glue_job_servicio" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "LeerYEscribirElLakehouse"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }

  statement {
    sid       = "ListarElBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid       = "LeerElDsnDePostgres"
    actions   = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = ["arn:aws:ssm:${var.region}:${data.aws_caller_identity.actual.account_id}:parameter/ypf-lakehouse/*"]
  }

  # El SecureString usa la clave por defecto de SSM (alias/aws/ssm), que no se puede nombrar
  # por ARN sin buscarla: se limita por el servicio que hace la llamada.
  statement {
    sid       = "DescifrarConLaClaveDeSsm"
    actions   = ["kms:Decrypt"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }

  statement {
    sid       = "EscribirLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${data.aws_caller_identity.actual.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "lakehouse"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# --- rol de la máquina de estados -------------------------------------------

resource "aws_iam_role" "step_functions" {
  name               = "${var.project}-stepfunctions"
  description        = "Rol de la máquina de estados produccion_pozo_mensual."
  assume_role_policy = data.aws_iam_policy_document.asume["states.amazonaws.com"].json
}

data "aws_iam_policy_document" "step_functions" {
  statement {
    sid     = "CorrerLosJobsDeGlue"
    actions = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [
      aws_glue_job.ingest_produccion_pozo.arn,
      aws_glue_job.bronze_produccion_pozo.arn,
      aws_glue_job.silver_produccion_pozo.arn,
    ]
  }

  # El patrón `.sync` de Step Functions se apoya en una regla administrada de EventBridge
  # para enterarse de que el job terminó.
  statement {
    sid       = "ReglaAdministradaDeEventBridge"
    actions   = ["events:PutRule", "events:PutTargets", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.region}:${data.aws_caller_identity.actual.account_id}:rule/StepFunctionsGetEventsForGlueJobRule"]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "correr-glue"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions.json
}

# --- rol del scheduler ------------------------------------------------------

resource "aws_iam_role" "scheduler" {
  name               = "${var.project}-scheduler"
  description        = "Rol que usa EventBridge Scheduler para arrancar la máquina de estados."
  assume_role_policy = data.aws_iam_policy_document.asume["scheduler.amazonaws.com"].json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    sid       = "ArrancarLaMaquinaDeEstados"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.produccion_pozo_mensual.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "arrancar-la-maquina"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}
