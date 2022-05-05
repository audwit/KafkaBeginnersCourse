package com.github.audwit.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
		
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "second_application";
		String topic = "first_topic";
		//String[] topics = new String[] {"firsttopic", "secondtopic"};
		//topics = ;
		
		//Create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		// Consumer reads for only new messages or from the beginning, latest for only new messages
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		
		//Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//Subscribe consumer to topics
		consumer.subscribe(Arrays.asList(topic));
		
		//Poll with consumer
		while(true){
		ConsumerRecords<String,String> records = consumer.poll(1000);
		for(ConsumerRecord<String, String> record :records) {
			logger.info("Partition" + record.partition());
			logger.info("Offset" + record.offset());
			logger.info("Topic" + record.topic());
			logger.info("Key" + record.key());
			logger.info("Message" + record.value());
		}
		}
	}

}
