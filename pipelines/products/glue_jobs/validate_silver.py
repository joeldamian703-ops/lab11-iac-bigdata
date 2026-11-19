
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# Inicializar Spark y AWS Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Configurar Apache Iceberg
spark.conf.set(
    "spark.sql.catalog.lakehouse",
    "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set(
    "spark.sql.catalog.lakehouse.type",
    "hadoop"
)
spark.conf.set(
    "spark.sql.catalog.lakehouse.warehouse",
    "s3a://ucv-data-platform/products"
)

# Conectar Spark con Floci
hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://floci:4566")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.access.key", "test")
hconf.set("fs.s3a.secret.key", "test")
hconf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

# Leer la tabla Silver
silver = spark.table("lakehouse.silver.products")

# Validar calidad de datos
total = silver.count()

null_ids = silver.filter(
    F.col("product_id").isNull()
).count()

invalid_prices = silver.filter(
    F.col("price").isNull() | (F.col("price") <= 0)
).count()

duplicates = (
    silver.groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\n========== VALIDACION SILVER ==========")
print(f"Total de productos: {total}")
print(f"IDs nulos: {null_ids}")
print(f"Precios invalidos: {invalid_prices}")
print(f"IDs duplicados: {duplicates}")

if null_ids == 0 and invalid_prices == 0 and duplicates == 0:
    print("RESULTADO: VALIDACION EXITOSA")
else:
    print("RESULTADO: SE DETECTARON PROBLEMAS")

print("========================================\n")

spark.stop()
