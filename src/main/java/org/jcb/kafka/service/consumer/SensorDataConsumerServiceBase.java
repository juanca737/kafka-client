/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jcb.kafka.controller.SensorDataClient;
import org.jcb.kafka.schema.SensorData;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public abstract class SensorDataConsumerServiceBase implements SensorDataConsumer {

    @Autowired
    private ConsumerPropertiesFactory consumerPropertiesFactory;

    protected Consumer<Integer, SensorData> consumer;

    private boolean subscribed;

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
        if (!subscribed) {
            consumer.subscribe(Collections.singleton(topic));
            subscribed = true;
        }
        ConsumerRecords<Integer, SensorData> messages
                = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

        List<SensorDataClient> result = new ArrayList<>();
        for (ConsumerRecord<Integer, SensorData> consumerRecord : messages.records(topic)) {
            SensorDataClient sensorData = new SensorDataClient();
            sensorData.setBuildingId(consumerRecord.key());
            sensorData.setSensorId(consumerRecord.value().getSensorId());
            sensorData.setTemperature(consumerRecord.value().getTemperature());
            sensorData.setStatus(consumerRecord.value().getStatus().toString());
            sensorData.setLastUpdate(consumerRecord.value().getLastUpdate());
            sensorData.setTopic(consumerRecord.topic());
            sensorData.setPartition(consumerRecord.partition());
            result.add(sensorData);
        }
        return result;
    }

}
