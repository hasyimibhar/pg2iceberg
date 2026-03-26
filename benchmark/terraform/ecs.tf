# ECR repositories.
resource "aws_ecr_repository" "pg2iceberg" {
  name                 = "${var.project}/pg2iceberg"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "iceberg_rest" {
  name                 = "${var.project}/iceberg-rest"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "bench_writer" {
  name                 = "${var.project}/bench-writer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "bench_verify" {
  name                 = "${var.project}/bench-verify"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

# ECS cluster.
resource "aws_ecs_cluster" "main" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# IAM role for ECS task execution (pulling images, logging).
resource "aws_iam_role" "ecs_execution" {
  name = "${var.project}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# IAM role for ECS tasks (S3 access for pg2iceberg and bench-verify).
resource "aws_iam_role" "ecs_task" {
  name = "${var.project}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_s3" {
  name = "${var.project}-s3-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
      ]
      Resource = [
        aws_s3_bucket.warehouse.arn,
        "${aws_s3_bucket.warehouse.arn}/*",
      ]
    }]
  })
}

# CloudWatch log group.
resource "aws_cloudwatch_log_group" "main" {
  name              = "/ecs/${var.project}"
  retention_in_days = 7
}

# Service discovery so tasks can find iceberg-rest by DNS.
resource "aws_service_discovery_private_dns_namespace" "main" {
  name = "${var.project}.local"
  vpc  = aws_vpc.main.id
}

resource "aws_service_discovery_service" "iceberg_rest" {
  name = "iceberg-rest"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

# --- Task Definitions ---

resource "aws_ecs_task_definition" "iceberg_rest" {
  family                   = "${var.project}-iceberg-rest"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.iceberg_rest_cpu
  memory                   = var.iceberg_rest_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "iceberg-rest"
    image     = "${aws_ecr_repository.iceberg_rest.repository_url}:latest"
    essential = true

    portMappings = [{
      containerPort = 8181
      protocol      = "tcp"
    }]

    environment = [
      { name = "AWS_REGION", value = var.region },
      { name = "CATALOG_CATALOG__IMPL", value = "org.apache.iceberg.jdbc.JdbcCatalog" },
      { name = "CATALOG_URI", value = "jdbc:postgresql://${aws_db_instance.catalog.address}:5432/iceberg_catalog" },
      { name = "CATALOG_JDBC_USER", value = "iceberg" },
      { name = "CATALOG_JDBC_PASSWORD", value = var.catalog_db_password },
      { name = "CATALOG_WAREHOUSE", value = "s3://${aws_s3_bucket.warehouse.id}/" },
      { name = "CATALOG_IO__IMPL", value = "org.apache.iceberg.aws.s3.S3FileIO" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "iceberg-rest"
      }
    }
  }])
}

locals {
  iceberg_rest_url = "http://iceberg-rest.${var.project}.local:8181"
}

resource "aws_ecs_task_definition" "pg2iceberg" {
  family                   = "${var.project}-pg2iceberg"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.pg2iceberg_cpu
  memory                   = var.pg2iceberg_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "pg2iceberg"
    image     = "${aws_ecr_repository.pg2iceberg.repository_url}:latest"
    essential = true

    entryPoint = ["sh", "-c"]
    command = [join("\n", [
      "cat > /tmp/config.yaml << 'CFGEOF'",
      "source:",
      "  mode: logical",
      "  postgres:",
      "    host: ${aws_db_instance.source.address}",
      "    port: 5432",
      "    database: bench",
      "    user: postgres",
      "    password: ${var.source_db_password}",
      "  logical:",
      "    publication_name: pg2iceberg_bench",
      "    slot_name: pg2iceberg_bench_slot",
      "    tables:",
      "      - name: public.bench_events",
      "sink:",
      "  catalog_uri: ${local.iceberg_rest_url}",
      "  warehouse: s3://${aws_s3_bucket.warehouse.id}/",
      "  namespace: benchmark",
      "  s3_region: ${var.region}",
      "  flush_interval: 10s",
      "  flush_rows: 1000",
      "  flush_bytes: 67108864",
      "state: {}",
      "metrics_addr: \":9090\"",
      "CFGEOF",
      "exec pg2iceberg --config /tmp/config.yaml",
    ])]

    portMappings = [{
      containerPort = 9090
      protocol      = "tcp"
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "pg2iceberg"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "bench_writer" {
  family                   = "${var.project}-bench-writer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.bench_writer_cpu
  memory                   = var.bench_writer_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "bench-writer"
    image     = "${aws_ecr_repository.bench_writer.repository_url}:latest"
    essential = true

    environment = [
      { name = "DATABASE_URL", value = "postgres://postgres:${var.source_db_password}@${aws_db_instance.source.address}:5432/bench" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "bench-writer"
      }
    }
  }])
}

resource "aws_ecs_task_definition" "bench_verify" {
  family                   = "${var.project}-bench-verify"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.bench_verify_cpu
  memory                   = var.bench_verify_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "bench-verify"
    image     = "${aws_ecr_repository.bench_verify.repository_url}:latest"
    essential = true

    environment = [
      { name = "DATABASE_URL", value = "postgres://postgres:${var.source_db_password}@${aws_db_instance.source.address}:5432/bench" },
      { name = "S3_REGION", value = var.region },
      { name = "CATALOG_URI", value = "http://iceberg-rest.${var.project}.local:8181" },
      { name = "WAREHOUSE", value = "s3://${aws_s3_bucket.warehouse.id}/" },
      { name = "NAMESPACE", value = "benchmark" },
      { name = "TABLE", value = "bench_events" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.main.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "bench-verify"
      }
    }
  }])
}

# --- Services ---

# iceberg-rest runs as a long-lived service with service discovery.
resource "aws_ecs_service" "iceberg_rest" {
  name            = "iceberg-rest"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.iceberg_rest.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public.id]
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = true
  }

  service_registries {
    registry_arn = aws_service_discovery_service.iceberg_rest.arn
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}
