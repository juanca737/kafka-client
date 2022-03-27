/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jcb.kafka.schema.SensorData;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;

public abstract class SensorDataProducerServiceBase {

    @Autowired
    protected ProducerPropertiesFactory producerPropertiesFactory;

    protected Producer<Integer, SensorData> producer;

    @PostConstruct
    void init() {
        Properties properties = producerPropertiesFactory.create();
        setExtraProperties(properties);
        producer = new KafkaProducer<>(properties);
    }

    protected abstract void setExtraProperties(Properties properties);

    @PreDestroy
    void preDestroy() {
        producer.flush();
        producer.close();
    }

    public void produce(String topic, Integer key, SensorData value, Callback callback) {
        producer.send(new ProducerRecord<>(topic, key, value), callback);
    }

}
