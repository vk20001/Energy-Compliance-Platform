from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("verify-bronze")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BRONZE_PATH = "/home/vaishu19/energy-compliance-platform/datalake/bronze/meter_readings"

df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Total rows: {df.count()}")
df.printSchema()
df.limit(1).show(truncate=False)
spark.stop()
