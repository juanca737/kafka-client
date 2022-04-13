/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class SensorDataPartitionedProducerService extends SensorDataProducerServiceBase {

    @Override
    protected void setExtraProperties(Properties properties) {
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-partitioned");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    }

}
