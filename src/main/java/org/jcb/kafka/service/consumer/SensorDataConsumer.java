/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.jcb.kafka.controller.ClientSensorData;

import java.util.List;

public interface SensorDataConsumer {

    void consume(String topic, MessageConsumerCallback<ClientSensorData> callback);

    List<ClientSensorData> consume(String topic);

}
