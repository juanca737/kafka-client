/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service;

import org.jcb.kafka.schema.SensorData;
import org.jcb.kafka.service.producer.ProducerCallBack;
import org.jcb.kafka.service.producer.SensorDataPartitionedProducerService;
import org.jcb.kafka.service.producer.SensorDataProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.Random;

/**
 * Create and send a batch of messages with random data for testing purposes
 */
@Service
public class BatchSend {

    @Autowired
    private SensorDataProducerService producerService;

    @Autowired
    private SensorDataPartitionedProducerService partitionedProducerService;

    @Autowired
    private ProducerCallBack producerCallBack;

    private final Random random = new SecureRandom();

    public void sendSimple(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            producerService.produce("topic-1", 1, buildData(), producerCallBack);
        }
    }

    public void sendPartitioned(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            partitionedProducerService.produce("topic-2", getRandomInt(1, 500), buildData(), producerCallBack);
        }
    }

    private SensorData buildData() {
        return SensorData.newBuilder()
                .setSensorId(getRandomInt(1, 100))
                .setTemperature(getRandomDouble(16.0, 28.0))
                .setStatus("UP")
                .setLastUpdate(System.currentTimeMillis())
                .build();
    }

    int getRandomInt(int min, int max) {
        return random.nextInt(max - min) + min;
    }

    double getRandomDouble(double min, double max) {
        return (random.nextDouble() * (max - min)) + min;
    }

}
