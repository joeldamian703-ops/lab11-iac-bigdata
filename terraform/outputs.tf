output "data_lake_bucket" {
  description = "Nombre del bucket S3 del Data Lake"
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_databases" {
  description = "Bases de datos creadas en Glue"
  value = {
    for domain, database in aws_glue_catalog_database.domains :
    domain => database.name
  }
}

output "floci_endpoint" {
  description = "Endpoint local de Floci"
  value       = "http://localhost:4566"
}
