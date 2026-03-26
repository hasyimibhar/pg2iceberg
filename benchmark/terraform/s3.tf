resource "aws_s3_bucket" "warehouse" {
  bucket_prefix = "${var.project}-warehouse-"
  force_destroy = true

  tags = { Name = "${var.project}-warehouse" }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration {
    status = "Disabled"
  }
}
