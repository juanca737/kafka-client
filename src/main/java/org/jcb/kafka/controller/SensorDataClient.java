/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.controller;

import lombok.Data;

@Data
public class SensorDataClient {

    private int buildingId;
    private int sensorId;
    private double temperature;
    private String status;
    private long lastUpdate;
    private String topic;
    private int partition;

}
