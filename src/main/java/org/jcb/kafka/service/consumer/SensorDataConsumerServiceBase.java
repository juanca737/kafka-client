/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jcb.kafka.controller.ClientSensorData;
import org.jcb.kafka.schema.SensorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class SensorDataConsumerServiceBase implements SensorDataConsumer {

    public static final Logger LOGGER = LoggerFactory.getLogger(SensorDataConsumerServiceBase.class);

    private static final int MAX_EMPTY_TRIES = 3;

    protected Consumer<Integer, SensorData> consumer;
    private boolean subscribed;

    protected SensorDataConsumerServiceBase(ConsumerPropertiesFactory consumerPropertiesFactory) {
        Properties properties = consumerPropertiesFactory.create();
        setExtraProperties(properties);
        consumer = new KafkaConsumer<>(properties);
    }

    protected abstract void setExtraProperties(Properties properties);

    @PreDestroy
    void preDestroy() {
        consumer.close();
    }

    public void consume(String topic, MessageConsumerCallback<ClientSensorData> callback) {
        if (!subscribed) {
            consumer.subscribe(Collections.singleton(topic));
            subscribed = true;
        }

        int emptyResultCount = 0;
        while (emptyResultCount <= MAX_EMPTY_TRIES) {
            ConsumerRecords<Integer, SensorData> messages
                    = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

            if (messages.isEmpty()) {
                emptyResultCount++;
            } else {
                emptyResultCount = 0;
            }

            processRecords(messages, callback);
        }
    }

    @Override
    public List<ClientSensorData> consume(String topic) {
        if (!subscribed) {
            consumer.subscribe(Collections.singleton(topic));
            subscribed = true;
        }

        ConsumerRecords<Integer, SensorData> messages
                = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));

        List<ClientSensorData> result = new ArrayList<>();
        processRecords(messages, result::add);
        return result;
    }

    private void processRecords(ConsumerRecords<Integer, SensorData> consumerRecords,
                                MessageConsumerCallback<ClientSensorData> callback) {
        consumerRecords.forEach(consumerRecord -> {
            ClientSensorData clientSensorData = new ClientSensorData();
            clientSensorData.setBuildingId(consumerRecord.key());
            clientSensorData.setSensorId(consumerRecord.value().getSensorId());
            clientSensorData.setTemperature(consumerRecord.value().getTemperature());
            clientSensorData.setStatus(consumerRecord.value().getStatus().toString());
            clientSensorData.setLastUpdate(consumerRecord.value().getLastUpdate());
            clientSensorData.setTopic(consumerRecord.topic());
            clientSensorData.setPartition(consumerRecord.partition());
            callback.call(clientSensorData);
        });
    }

}
