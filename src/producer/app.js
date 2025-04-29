const { Kafka } = require('kafkajs');
const fs = require('fs');
const config = require('./config.json');

// Create a new Kafka client
const kafka = new Kafka({
  clientId: 'kafka-producer',
  brokers: [config.kafkaBroker],  // Use Kafka broker from config
});

// Create a Kafka producer
const producer = kafka.producer();

// Function to generate random mock data
const generateData = () => {
  return {
    id: Math.floor(Math.random() * 1000),
    message: `Message #${Math.floor(Math.random() * 1000)}`,
    timestamp: new Date().toISOString(),
  };
};

// Main function to start the producer and send data
const startProducer = async () => {
  await producer.connect();
  console.log('Kafka Producer connected!');

  setInterval(async () => {
    const message = generateData();
    console.log(`Sending message: ${JSON.stringify(message)}`);

    // Send message to Kafka topic
    await producer.send({
      topic: config.kafkaTopic,  // The Kafka topic to send to
      messages: [
        {
          value: JSON.stringify(message),  // Convert data to JSON string
        },
      ],
    });
  }, 1000); // Send data every second
};

// Start the producer
startProducer().catch(console.error);
