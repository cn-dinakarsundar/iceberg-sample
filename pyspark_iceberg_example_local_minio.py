from pyspark.sql import SparkSession

spark =(
    SparkSession.builder
    .appName("IcebergwithSPark")
    # .config("spark.sql.catalog.local", "org.apache.iceberg.spark.catalog.SparkCatalog")
    # .config("spark.sql.catalog.local.type", "hadoop")
    # .config("spark.sql.catalog.local.warehouse", "/tmp/warehouse")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-bucket/warehouse")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")

    .getOrCreate()
)


# Create Iceberg table
spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.default.sample_table (
    id INT,
    name STRING,
    age INT
) USING iceberg
""")


# Insert data with DataFrame
df = spark.createDataFrame([
    (1, "Alice", 30),
    (2, "Bob", 28),
    (3, "Charlie", 25),
], ["id", "name", "age"])

df.writeTo("spark_catalog.default.sample_table").append()

# Query table
spark.sql("SELECT * FROM spark_catalog.default.sample_table").show()
