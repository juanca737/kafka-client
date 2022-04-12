/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jcb.kafka.controller.SensorDataClient;
import org.jcb.kafka.schema.SensorData;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class SensorDataConsumerServiceBase implements SensorDataConsumer {

    private static final int MAX_EMPTY_TRIES = 100;

    @Autowired
    private ConsumerPropertiesFactory consumerPropertiesFactory;

    protected Consumer<Integer, SensorData> consumer;

    @PostConstruct
    void init() {
        Properties properties = consumerPropertiesFactory.create();
        setExtraProperties(properties);
        consumer = new KafkaConsumer<>(properties);
    }

    protected abstract void setExtraProperties(Properties properties);

    @PreDestroy
    void preDestroy() {
        consumer.close();
    }

    @Override
    public List<SensorDataClient> consume(String topic) {
        List<SensorDataClient> result = new ArrayList<>();
        int emptyResultCount = 0;

        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<Integer, SensorData> messages
                    = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

            if (messages.isEmpty() && emptyResultCount++ > MAX_EMPTY_TRIES) {
                break;
            }

            messages.records((topic)).forEach(consumerRecord -> {
                SensorDataClient sensorData = new SensorDataClient();
                sensorData.setBuildingId(consumerRecord.key());
                sensorData.setSensorId(consumerRecord.value().getSensorId());
                sensorData.setTemperature(consumerRecord.value().getTemperature());
                sensorData.setStatus(consumerRecord.value().getStatus().toString());
                sensorData.setLastUpdate(consumerRecord.value().getLastUpdate());
                sensorData.setTopic(consumerRecord.topic());
                sensorData.setPartition(consumerRecord.partition());
                result.add(sensorData);
            });
        }
        return result;
    }

}
