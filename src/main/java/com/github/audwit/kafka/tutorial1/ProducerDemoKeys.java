package com.github.audwit.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		
		System.out.println("Hello world");
		
		String bootstrapServers = "127.0.0.1:9092";
		
		String topic = "first_topic";
		String key;
		String message = "Hello!";
		
		//Create producer properties
		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", bootstrapServers);
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		properties.setProperty("value.serializer", StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// Create producer record
		for(int i=0; i<10; i++) {
			
		key = Integer.toString(i);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message + i);
		
		logger.info("The key is:" + key);
		
		//Send data. As this is asynchronous this will not work until flushing or closing the producer
		producer.send(record, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// This method above is called every time a record is sent or an exception is thrown
				if(exception==null) {
					logger.info("Received new metadata \n" +
							"Topic: " + metadata.topic() + "\n" +
							"partition: " + metadata.partition() + "\n" +
							"Offset: " + metadata.offset() + "\n" +
							"Timestamp: " + metadata.timestamp() + "\n"
					);
				}
				else
					logger.error("Error while producing " + exception);
			}
		}).get();
		
		}
		
		// Flush producer
		producer.flush();
		
		// Close and flush producer
		producer.close();
		
		
	}

}
