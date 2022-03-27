/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class SensorDataConsumerService extends SensorDataConsumerServiceBase {

    @Override
    protected void setExtraProperties(Properties properties) {
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-simple");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-simple-group-1");
    }

}
