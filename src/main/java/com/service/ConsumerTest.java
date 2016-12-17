package com.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerTest {

	private ConsumerConnector consumer;
	private String topic;

	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.198.128:2181");
		props.put("group.id", "testgroup");
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public void testConsumer() {
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while (consumerIte.hasNext()){
				System.out.println("Message from Single Topic :: " + new String(consumerIte.next().message()));
			}
		}
		if (consumer != null)
			consumer.shutdown();
	}

	public void simpleHLConsumer(String zookeeper, String groupId, String topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
		this.topic = topic;
	}

	public static void main(String[] args) {
		ConsumerTest consumerTest = new ConsumerTest();
		consumerTest.simpleHLConsumer("192.168.198.128", "testGroup", "test");
		consumerTest.testConsumer();
	}
}
