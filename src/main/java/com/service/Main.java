package com.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

@SuppressWarnings("all")
public class Main {

	private final static String TOPIC = "test";
	private static Producer<String, String> producer;

	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.put("metadata.broker.list","192.168.198.128:9092");		
		props.put("serializer.class", "kafka.serializer.StringEncoder");		
		props.put("request.required.acks", "1");
		props.put("partitioner.class","com.part.SimplePartition");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);

		Random random = new Random();
		for (int i = 0; i < 10; i++) {
			String clientIP = "192.168.14." + random.nextInt(255);
			long runtime = new Date().getTime();
			String msg = "Message Publishing Time - " + runtime;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, clientIP,msg);
			producer.send(data);
		}
		producer.close();
	}
}
