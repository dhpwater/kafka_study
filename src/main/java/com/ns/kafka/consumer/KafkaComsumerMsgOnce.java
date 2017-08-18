package com.ns.kafka.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.javaapi.PartitionMetadata;


public class KafkaComsumerMsgOnce {

	private List<String> m_replicaBrokers = new ArrayList<String>();

	public KafkaComsumerMsgOnce() {
		m_replicaBrokers = new ArrayList<String>();
	}

	private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
		
		KafkaConsumer consumer = null ;
//		consumer = new 
		
		return null; 
	}

	public static void main(String[] args) {

	}

}
