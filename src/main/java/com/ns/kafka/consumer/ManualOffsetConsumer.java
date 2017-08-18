package com.ns.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.kafka.KafkaProperties;

public class ManualOffsetConsumer {
	private static Logger logger = LoggerFactory.getLogger(ManualOffsetConsumer.class);

	public ManualOffsetConsumer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		// 设置brokerServer(kafka)ip地址
		props.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "70000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "80000");
		
		// 设置consumer group name
		props.put("group.id", "manual_g1");

		props.put("enable.auto.commit", "false");

		// 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
		// 如果采用latest，消费者只能得道其启动后，生产者生产的消息
		props.put("auto.offset.reset", "earliest");
		
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//		consumer.subscribe(Arrays.asList("producer_test"));
		
		consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC));
		final int minBatchSize = 5; // 批量提交数量
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				logger.info("consumer message values is " + record.value() + " and the offset is " + record.offset());
				buffer.add(record);
			}
//			logger.info("buffer.size :" + buffer.size());
			if (buffer.size() >= minBatchSize) {
				logger.info("now commit offset");
				consumer.commitSync();
				buffer.clear();
			}
		}
	}

}
