package com.ns.kafka.sample;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alibaba.fastjson.JSON;
import com.ns.kafka.KafkaProperties;

import kafka.utils.ShutdownableThread;


/**
 * 
 * group.id :必须设置 
   auto.offset.reset：如果想获得消费者启动前生产者生产的消息，则必须设置为earliest；
       如果只需要获得消费者启动后生产者生产的消息，则不需要设置该项 enable.auto.commit(默认值为true)：
       如果使用手动commit offset则需要设置为false，并再适当的地方调用consumer.commitSync()，
       否则每次启动消费折后都会从头开始消费信息(在auto.offset.reset=earliest的情况下);
 * @author ryan
 *
 */
public class Consumer extends ShutdownableThread {

	private final KafkaConsumer<Integer, String> consumer;
	private final String topic;

	public Consumer(String topic) {
		super("KafkaConsumerExample", false);
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "70000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "80000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		
		//设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
		//如果采用latest，消费者只能得道其启动后，生产者生产的消息
		props.put("auto.offset.reset", "earliest");
		

		consumer = new KafkaConsumer<>(props);
		this.topic = topic;
	}

	@Override
	public void doWork() {
		// TODO Auto-generated method stub
		
		consumer.subscribe(Collections.singletonList(this.topic));
		
		while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer, String> record : records){
            	String val = record.value();
            	Person p = JSON.parseObject(val, Person.class);
            	System.out.println(p);
                System.out.printf("topic=%s, partition=%s, offset = %d, key = %s, value = %s\n", record.topic(),record.partition(),record.offset(), record.key(), record.value());
            }
        }
	}

	@Override
	public String name() {
		return null;
	}

	@Override
	public boolean isInterruptible() {
		return false;
	}

}
