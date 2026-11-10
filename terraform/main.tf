# Crear el bucket S3 para el Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket

  tags = {
    Environment = var.environment
    Project     = "UCV-BigData-Lab11"
  }
}

# Generar las rutas de cada dominio y capa
locals {
  prefixes = flatten([
    for domain in var.domains : [
      for layer in var.layers : {
        key = "${domain}/${layer}/"
      }
    ]
  ])
}

# Crear los prefijos dentro del bucket S3
resource "aws_s3_object" "folders" {
  for_each = {
    for prefix in local.prefixes : prefix.key => prefix
  }

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value.key
  content = ""
}

# Crear una base de datos Glue por dominio
resource "aws_glue_catalog_database" "domains" {
  for_each = var.domains

  name       = "${var.environment}_${each.value}"
  catalog_id = var.catalog_id
}
