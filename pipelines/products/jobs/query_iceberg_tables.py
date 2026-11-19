
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Inicializar Spark y Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Configurar el catalogo Apache Iceberg
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

# Configurar conexion con Floci S3
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

# Consultar tabla Silver
print("\n========== TABLA SILVER ==========")

silver = spark.table("lakehouse.silver.products")

silver.printSchema()
silver.show(10, truncate=False)

print(f"Total de productos Silver: {silver.count()}")

# Consultar tabla Gold
print("\n========== TABLA GOLD ==========")

gold = spark.table(
    "lakehouse.gold.product_category_summary"
)

gold.printSchema()
gold.show(30, truncate=False)

print(f"Total de categorias Gold: {gold.count()}")

print("\nLECTURA DE TABLAS ICEBERG EXITOSA")

spark.stop()
