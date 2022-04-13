/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.jcb.kafka.configuration.AppConfiguration;
import org.jcb.kafka.service.consumer.ConsumerPropertiesFactory;
import org.jcb.kafka.service.consumer.SensorDataConsumer;
import org.jcb.kafka.service.consumer.SensorDataConsumerService;
import org.jcb.kafka.service.consumer.SensorDataPartitionedConsumerService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerApp {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerApp.class);

    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        AppConfiguration configuration = getAppConfiguration();
        startSimpleConsumer(configuration);
        startPartitionedConsumer(configuration);
    }

    @NotNull
    private static AppConfiguration getAppConfiguration() {
        AppConfiguration configuration = new AppConfiguration();
        configuration.setBrokers("localhost:29092");
        configuration.setSchemaRegistryUrl("http://localhost:8081");
        configuration.setConsumerMaxPollRecords(100);
        configuration.setConsumerOffsetResetEarlier("earliest");
        return configuration;
    }

    private static void startSimpleConsumer(AppConfiguration configuration) {
        SensorDataConsumer consumer = new SensorDataConsumerService(new ConsumerPropertiesFactory(configuration));

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(() ->
            consumer.consume("topic-1", data -> LOGGER.info(data.toString())), 1, 5, TimeUnit.SECONDS);
    }

    private static void startPartitionedConsumer(AppConfiguration configuration) {
        SensorDataConsumer consumer = new SensorDataPartitionedConsumerService(new ConsumerPropertiesFactory(configuration));
        ScheduledExecutorService executorService2 = Executors.newSingleThreadScheduledExecutor();
        executorService2.scheduleAtFixedRate(() ->
            consumer.consume("topic-2", data -> LOGGER.info(data.toString())), 1, 5, TimeUnit.SECONDS);
    }

}
