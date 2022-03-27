/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.configuration;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SchemaRegistryClientConfiguration {

    @Autowired
    private AppConfiguration configuration;

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        return new CachedSchemaRegistryClient(configuration.getSchemaRegistryUrl(), 20);
    }

}
