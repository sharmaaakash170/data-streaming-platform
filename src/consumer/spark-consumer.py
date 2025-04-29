from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import time

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka-service.default.svc.cluster.local:9093"
TOPIC = "test-topic"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints"  # For fault tolerance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session with Kafka packages"""
    return SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.streaming.kafka.maxRetries", "5") \
        .config("spark.streaming.kafka.retry.backoff.ms", "1000") \
        .getOrCreate()

def wait_for_kafka(max_retries=5, delay_seconds=10):
    """Wait for Kafka to become available"""
    from kafka import KafkaConsumer
    for attempt in range(max_retries):
        try:
            KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=3000
            )
            logger.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} - Kafka not ready: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(delay_seconds)
    return False

def process_stream(spark):
    """Create and manage the streaming query"""
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()

        # Process messages
        messages = df.select(
            col("key").cast("string"),
            col("value").cast("string"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        )

        query = messages.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        logger.info("Stream processing started")
        return query

    except Exception as e:
        logger.error(f"Failed to create streaming query: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        logger.info("Initializing Spark Kafka consumer")
        
        # Wait for Kafka to be ready
        if not wait_for_kafka():
            raise RuntimeError("Failed to connect to Kafka after multiple attempts")
        
        spark = create_spark_session()
        query = process_stream(spark)
        
        # Add shutdown hook
        def shutdown_hook():
            logger.info("Stopping streaming query")
            query.stop()
        
        import atexit
        atexit.register(shutdown_hook)
        
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        raise