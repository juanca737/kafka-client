/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ProducerCallBack implements Callback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerCallBack.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            LOGGER.error("Unable to deliver message to topic {}.", metadata.topic(), e);
        } else {
            LOGGER.info("Message delivered to topic {}, offset {}.", metadata.topic(), metadata.offset());
        }
    }

}
