/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null || !(key instanceof Integer)) {
            throw new InvalidRecordException("Key must be of type Integer");
        }
        return getPartition((int) key);
    }

    private int getPartition(int buildingId) {
        int partition;
        if (buildingId < 100) {
            partition = 0;
        } else if (buildingId < 200) {
            partition = 1;
        } else if (buildingId < 300) {
            partition = 2;
        } else {
            partition = 3;
        }
        return partition;
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // nothing to do
    }

}
