package com.part;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartition implements Partitioner {

	public SimplePartition(VerifiableProperties props) {
	
	}
	
	@Override
	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String partitionKey = (String) key;
		int offset = partitionKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(partitionKey.substring(offset + 1)) % a_numPartitions;
		}
		return partition;
	}
}
