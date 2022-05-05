package com.github.audwit.kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ConsumerDemoWithThreadSelf {

	public static void main(String[] args) {
		new ConsumerDemoWithThreadSelf().run();
	}

	private void run(){
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreadSelf.class.getName());
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "fourth_application";
		String topic = "first_topic";
		CountDownLatch latch = new CountDownLatch(1);
		Runnable myConsumerThread = new ConsumerThread(bootstrapServers, groupId, topic, latch);
		//Start the consumer thread to consume
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();

		// Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			((ConsumerThread)myConsumerThread).shutdown();
			try {
					latch.await();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));
	}


	public class ConsumerThread implements Runnable{
		KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.ConsumerRunnable.class.getName());
		CountDownLatch latch;

		public ConsumerThread(String bootstrapServers, String groupId, String topic, CountDownLatch latch){

			Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreadSelf.class.getName());
			this.latch = latch;

			//String[] topics = new String[] {"firsttopic", "secondtopic"};
			//topics = ;

			//Create consumer config
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			// Consumer reads for only new messages or from the beginning, latest for only new messages
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			//Runnable myConsumerThread = new ConsumerThread(bootstrapServers, groupId, topic, latch);

			consumer = new KafkaConsumer<String, String>(properties);
			//Subscribe consumer to topics
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {
			try {
				//Poll with consumer
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(1000);
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Partition" + record.partition());
						logger.info("Offset" + record.offset());
						logger.info("Topic" + record.topic());
						logger.info("Key" + record.key());
						logger.info("Message" + record.value());
					}
				}
			}
			catch (WakeupException e){
				logger.info("Received shutdown signal");
			}
			finally {
				consumer.close();
				// Tell our main code we're done with consumer
				latch.countDown();
			}
		}

		public void shutdown(){
			// wakeup() is a special method to interrupt consumer.poll(). This throws a wakeup exception
			consumer.wakeup();
		}
	}


}
