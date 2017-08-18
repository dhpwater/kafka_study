package com.ns.kafka.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ns.kafka.KafkaProperties;
import com.ns.kafka.sample.Consumer;

public class KafkaConsumerMutiTest {

	public static void main(String[] args) {

		// 创建一个可重用固定线程数的线程池
		int threadCount = 5;
		ExecutorService pool = Executors.newFixedThreadPool(threadCount);
		
		for(int i =0 ; i < threadCount ; i ++ ){
			
			pool.execute(new Consumer(KafkaProperties.TOPIC));
		}
		
		
		pool.shutdown();
	}
}
