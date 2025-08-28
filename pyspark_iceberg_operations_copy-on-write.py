from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("IcebergUpdate")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")  
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") 
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///Users/dsundar/vse/projects/proj02/warehouse") 
    .getOrCreate() 
)


# Create Iceberg table
spark.sql("""
CREATE OR REPLACE TABLE spark_catalog.default.users_cow (
    id INT,
    name STRING,
    age INT
) USING iceberg
TBLPROPERTIES (
  'write.delete.mode'='copy-on-write',
  'write.update.mode'='copy-on-write'
)
""")


# Insert initial data
spark.sql("""
INSERT INTO spark_catalog.default.users_cow VALUES
(1, 'Alice', 25),
(2, 'Bob', 30),
(3, 'Charlie', 35)
""")

spark.sql("""
UPDATE spark_catalog.default.users_cow
SET age = age + 1
WHERE id = 2
""")

# Perform Delete (Copy-on-Write triggered)
spark.sql("""
DELETE FROM spark_catalog.default.users_cow
WHERE id = 1
""")


# Verify
df = spark.sql("SELECT * FROM spark_catalog.default.users_cow")
df.show()

