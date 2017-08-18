package com.ns.kafka.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ns.kafka.KafkaProperties;
import com.ns.kafka.sample.Producer;

public class KafkaProducerMutiTest {

	public static void main(String[] args){
		
		//创建一个可重用固定线程数的线程池
		int threadCount = 5 ;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        
        boolean isAsync = true;
        
        for(int i =0 ; i < threadCount ; i ++){
        	
        	pool.execute( new Producer(KafkaProperties.TOPIC, isAsync));
        }

        //关闭线程池
        pool.shutdown();

	}
}
