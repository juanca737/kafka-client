/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.controller;

import org.jcb.kafka.schema.SensorData;
import org.jcb.kafka.service.BatchSend;
import org.jcb.kafka.service.consumer.SensorDataConsumer;
import org.jcb.kafka.service.consumer.SensorDataConsumerService;
import org.jcb.kafka.service.consumer.SensorDataPartitionedConsumerService;
import org.jcb.kafka.service.producer.ProducerCallBack;
import org.jcb.kafka.service.producer.SensorDataPartitionedProducerService;
import org.jcb.kafka.service.producer.SensorDataProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(value = "/messages", produces = MediaType.APPLICATION_JSON_VALUE)
@ResponseBody
public class MessagesController {

    @Autowired
    private BatchSend batchSend;

    @Autowired
    private SensorDataProducerService producerService;

    @Autowired
    private SensorDataPartitionedProducerService partitionedProducerService;

    @Autowired
    private ProducerCallBack producerCallBack;

    @Autowired
    private SensorDataConsumerService consumerService;

    @Autowired
    private SensorDataPartitionedConsumerService consumerPartitionedService;

    @PostMapping("/{topic}")
    public ResponseEntity<?> sendMessage(@PathVariable String topic,  @RequestBody SensorDataClient sensorDataClient) {

        SensorData data =
                SensorData.newBuilder()
                        .setSensorId(sensorDataClient.getSensorId())
                        .setTemperature(sensorDataClient.getTemperature())
                        .setStatus(sensorDataClient.getStatus())
                        .setLastUpdate(System.currentTimeMillis())
                        .build();

        if (topic.equalsIgnoreCase("topic-1")) {
            producerService.produce(topic, sensorDataClient.getBuildingId(), data,  producerCallBack);
        } else {
            partitionedProducerService.produce(topic, sensorDataClient.getBuildingId(), data,  producerCallBack);
        }
        return ResponseEntity.ok("Message sent");
    }

    @GetMapping("/{topic}")
    public ResponseEntity<List<SensorDataClient>> getMessages(@PathVariable String topic) {
        List<SensorDataClient> result;
        if (topic.equalsIgnoreCase("topic-1")) {
            result = getAllMessages(consumerService, topic);
        } else {
            result = getAllMessages(consumerPartitionedService, topic);
        }
        return ResponseEntity.ok().body(result);
    }

    public List<SensorDataClient> getAllMessages(SensorDataConsumer consumer, String topic) {
        List<SensorDataClient> result = new ArrayList<>();

        int emptyCount = 0;
        List<SensorDataClient> simpleResult;
        while (true) {
            simpleResult = consumer.consume(topic);
            // I didn't find a cleaner way to do retrieve messages until the topic is empty...
            if (simpleResult.isEmpty()) {
                if (++emptyCount >= 5) {
                    break;
                }
                continue;
            }
            result.addAll(simpleResult);
        }
        return result;
    }

    @PostMapping()
    public ResponseEntity<?> sendBatchRandom() {
        batchSend.sendSimple(10);
        batchSend.sendPartitioned(50);
        return ResponseEntity.ok().build();
    }

}
