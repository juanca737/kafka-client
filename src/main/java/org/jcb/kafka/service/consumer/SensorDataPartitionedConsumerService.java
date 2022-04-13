/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class SensorDataPartitionedConsumerService extends SensorDataConsumerServiceBase {

    public SensorDataPartitionedConsumerService(@Autowired ConsumerPropertiesFactory consumerPropertiesFactory) {
        super(consumerPropertiesFactory);
    }

    @Override
    protected void setExtraProperties(Properties properties) {
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-partitioned");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-partitioned-group-2");
    }

}
