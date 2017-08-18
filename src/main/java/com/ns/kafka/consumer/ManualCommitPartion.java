package com.ns.kafka.consumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ns.kafka.KafkaProperties;

/**
 * 消费完一个分区后手动提交偏移量
 * 
 * @author ryan
 *
 */
public class ManualCommitPartion {
	private static Logger logger = LoggerFactory.getLogger(ManualCommitPartion.class);

	public ManualCommitPartion() {
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
		
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
				for (ConsumerRecord<String, String> record : partitionRecords) {
					logger.info("now consumer the message it's offset is :" + record.offset() + " and the value is :"
							+ record.value());
				}
				long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
				logger.info("now commit the partition[ " + partition.partition() + "] offset");
				consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}
		}
	}
}
