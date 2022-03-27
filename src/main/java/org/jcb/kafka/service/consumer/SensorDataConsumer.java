/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

import org.jcb.kafka.controller.SensorDataClient;

import java.util.List;

public interface SensorDataConsumer {

    List<SensorDataClient> consume(String topic);

}
