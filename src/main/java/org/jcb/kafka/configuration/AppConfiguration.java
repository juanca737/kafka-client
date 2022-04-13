/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@Setter
public class AppConfiguration {

    @Value("${kafka.brokers:localhost:29092}")
    private String brokers;

    @Value("${kafka.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    @Value("${kafka.consumer.max-poll-records:100}")
    private Integer consumerMaxPollRecords;

    @Value("${kafka.consumer.offset-reset-earlier:earliest}")
    private String consumerOffsetResetEarlier;

}
