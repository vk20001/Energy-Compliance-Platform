from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

REDPANDA_BOOTSTRAP = "d6sinugbmgg6innos300.any.eu-central-1.mpx.prd.cloud.redpanda.com:9092"
REDPANDA_TOPIC = "energy.public.meter_readings"
BRONZE_PATH = "/home/vaishu19/energy-compliance-platform/datalake/bronze/meter_readings"
CHECKPOINT_PATH = "/home/vaishu19/energy-compliance-platform/spark/checkpoints/bronze_meter_readings"

SASL_CONFIG = (
    'org.apache.kafka.common.security.scram.ScramLoginModule required '
    'username="energy-compliance-user" password="energypass123";'
)

spark = (SparkSession.builder
    .appName("bronze-meter-readings")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", REDPANDA_BOOTSTRAP)
    .option("subscribe", REDPANDA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", SASL_CONFIG)
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)

bronze_stream = (raw_stream
    .selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    )
    .withColumn("ingested_at", current_timestamp())
)

query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("path", BRONZE_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
print(f"Bronze ingestion complete.")

from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, BRONZE_PATH)
print(f"Bronze meter_readings row count: {dt.toDF().count()}")

spark.stop()
