from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("IcebergUpdate")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")  
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") 
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///Users/dsundar/vse/projects/proj02/warehouse") 
    .getOrCreate() 
)


# Assuming 'your_catalog' and 'your_database.your_table' are configured
# and 'updates_df' is a PySpark DataFrame containing the updates.

# Example: Create a DataFrame with updates
updates_data = [
    (1, "Updated Name 1", 35),
    (2, "Updated Name 2", 40)
]
updates_df = spark.createDataFrame(updates_data, ["id", "name", "age"])

updates_df.createOrReplaceTempView("updates_df")


# Perform the MERGE INTO operation
spark.sql(f"""
    MERGE INTO spark_catalog.default.sample_table AS target
    USING updates_df AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET target.name = source.name, target.age = source.age
    WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (source.id, source.name, source.age)
""")

#spark.stop()

spark.sql("SELECT * FROM spark_catalog.default.sample_table").show()

spark.stop()