package com.ns.kafka.producer;



public class KafkaProducerTest {

	public static void main(String[] args){
		
		boolean isAsync = true; 
		
		Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
		
        producerThread.start();
        
	}
}
