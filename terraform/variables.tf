variable "aws_region" {
  description = "Region AWS utilizada por Floci"
  type        = string
  default     = "us-east-1"
}

variable "data_lake_bucket" {
  description = "Nombre del bucket S3 del Data Lake"
  type        = string
  default     = "ucv-data-platform"
}

variable "environment" {
  description = "Entorno de despliegue"
  type        = string
  default     = "dev"
}

variable "domains" {
  description = "Dominios de datos"
  type        = set(string)
  default     = ["sales", "customers", "products", "marketing"]
}


variable "layers" {
  description = "Capas de la arquitectura Medallion"
  type        = set(string)
  default     = ["bronze", "silver", "gold"]
}

variable "catalog_id" {
  description = "ID de cuenta utilizado por Glue Catalog"
  type        = string
  default     = "000000000000"
}
