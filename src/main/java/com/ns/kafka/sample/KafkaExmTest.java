package com.ns.kafka.sample;

import com.ns.kafka.KafkaProperties;

public class KafkaExmTest {

	public static void main(String[] args) {

//		produce();

		consume();
	}
	
	public static void produce(){
		boolean isAsync = true;

		Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);

		producerThread.start();
	}
	
	public static void consume(){
		Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);

		consumerThread.start();
	}
}
