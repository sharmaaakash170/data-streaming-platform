FROM python:3.8-slim

# Install Kafka Python library
RUN pip install kafka-python

# Set the working directory
WORKDIR /app

# Copy the producer script into the container
COPY producer.py .

# Command to run the producer
CMD ["python", "producer.py"]
