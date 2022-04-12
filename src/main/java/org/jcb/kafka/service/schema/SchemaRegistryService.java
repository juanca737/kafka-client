/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

@Service
public class SchemaRegistryService {

    @Autowired
    private SchemaRegistryClient client;

    public void registerSchema(String schemaResourceName, String subject) throws IOException, RestClientException, SchemaResourceNotFound {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(schemaResourceName);
        if (resource == null) {
            throw new SchemaResourceNotFound("Resource not found: " + schemaResourceName);
        }
        String schemaFilePath = resource.getFile();

        String schemaContents;
        try (var inputStream = new FileInputStream(schemaFilePath)) {
            schemaContents = IOUtils.toString(inputStream, Charset.defaultCharset());
        }
        client.register(subject, new AvroSchema(schemaContents));
    }

    public void unregisterSchema(String subject) throws IOException, RestClientException {
        client.deleteSubject(subject);
    }

}
