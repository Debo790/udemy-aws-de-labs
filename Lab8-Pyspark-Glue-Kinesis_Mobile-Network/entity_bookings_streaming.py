import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import window, count, col, from_json, avg, to_timestamp, concat, lit, date_format,unix_timestamp
from awsglue.context import GlueContext
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

try:
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = SparkSession.builder.appName("KinesisDataAnalysis").getOrCreate()
    logger.info("Spark session created successfully.")

# Define the schema for the new dataset

    eventInfo_schema = StructType([
        StructField("eventId", StringType(), True),
        StructField("traceId", StringType(), True),
        StructField("startDate", StringType(), True),
        StructField("date", StringType(), True)
    ])
    schema = StructType([
        StructField("competitionId", StringType(), True),
        StructField("entityId", StringType(), True),
        StructField("oddsPhase", StringType(), True),
        StructField("entityType", StringType(), True),
        StructField("action", StringType(), True),
        StructField("eventInfo", eventInfo_schema, True),
        StructField("customerId", StringType(), True)
    ])

    # Read from Kinesis
    raw_data_frame = glueContext.create_data_frame.from_options(
        connection_type="kinesis",
        connection_options={
            "streamARN": "arn:aws:kinesis:eu-west-1:135053816219:stream/entity_booking",
            "classification": "json",
            "startingPosition": "trim_horizon",
            "inferSchema": "true"
        }
    )

    data_frame = raw_data_frame.select(
        from_json(col("$json$data_infer_schema$_temporary$"), schema).alias("parsed")).select("parsed.*")

    data_frame_with_timestamp = data_frame.withColumn(
        "timestamp",
        to_timestamp(col("eventInfo.date"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'")
    ).withColumn(
        "partition_hour",
        date_format(col("timestamp"), "HH")
    )

    # Extract hour for partitioning
    data_frame_with_partition_hour = data_frame_with_timestamp.withColumn(
        "partition_hour",
        date_format(col("timestamp"), "HH")
    )

    data_frame_with_watermark = data_frame_with_partition_hour.withWatermark("timestamp", "10 minutes")

    aggregated_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"),
        col("customerId"),
        col("entityType")
    ).agg(
        count("*").alias("event_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("customerId"),
        col("entityType"),
        col("event_count"),
        date_format(col("window.start"), "HH").alias("partition_hour")
    )

    s3_path = "s3://udemy-aws-dataeng-labs/aggregations/"
    s3_path_checkpoint = "s3://udemy-aws-dataeng-labs/checkpoints/"

    customer_entity = (aggregated_df.writeStream
                       .format("parquet")
                       .option("path", s3_path + "customer_entity/")
                       .option("checkpointLocation", s3_path_checkpoint + "customer_entity/")
                       .partitionBy("partition_hour", "customerId")
                       .outputMode("append")
                       .trigger(processingTime="20 seconds")
                       .start())

    logger.info("Starting the streaming jobs.")
    customer_entity.awaitTermination()

except Exception as e:
    logger.error("An error occurred: ", exc_info=True)