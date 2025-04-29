# Use the official Spark base image (e.g., Bitnami Spark)
FROM bitnami/spark:3.1.2

# Install required system packages and Python
USER root
RUN apt-get update && \
    apt-get install -y python3-pip python3-dev curl && \
    apt-get clean

# Install PySpark and kafka-python
RUN pip3 install --no-cache-dir pyspark kafka-python

# Download the Kafka Spark connector
RUN curl -sL https://repos.spark-packages.org/kafka/spark-sql-kafka-0-10_2.12-3.1.2.jar -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar

# Copy the consumer script into the container
COPY spark-consumer.py /opt/spark-apps/spark-consumer.py

# Set the working directory
WORKDIR /opt/spark-apps

# Set entry point to run the Spark consumer script
CMD ["spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "/opt/spark-apps/spark-consumer.py"]
