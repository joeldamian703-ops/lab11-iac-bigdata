
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

# Configurar conexion con Floci
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

# Leer la tabla Gold
gold = spark.table(
    "lakehouse.gold.product_category_summary"
)

# Validar los datos
total_categories = gold.count()

null_categories = gold.filter(
    F.col("category").isNull()
).count()

duplicate_categories = (
    gold.groupBy("category")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_counts = gold.filter(
    F.col("product_count").isNull()
    | (F.col("product_count") <= 0)
).count()

print("\n========== VALIDACION GOLD ==========")
print(f"Total de categorias: {total_categories}")
print(f"Categorias nulas: {null_categories}")
print(f"Categorias duplicadas: {duplicate_categories}")
print(f"Conteos invalidos: {invalid_counts}")

if (
    null_categories == 0
    and duplicate_categories == 0
    and invalid_counts == 0
):
    print("RESULTADO: VALIDACION EXITOSA")
else:
    print("RESULTADO: SE DETECTARON PROBLEMAS")

print("=====================================\n")

spark.stop()
