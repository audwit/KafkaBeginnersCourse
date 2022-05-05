package com.github.audwit.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
		
		System.out.println("Hello world");
		
		String bootstrapServers = "127.0.0.1:9092";
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
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Alien!");
		
		//Send data. As this is asynchronous this will not work until flushing or closing the producer
		producer.send(record);
		
		// Flush producer
		producer.flush();
		
		// Close and flush producer
		producer.close();
		
		
	}

}
