/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.controller;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.jcb.kafka.service.schema.SchemaResourceNotFound;
import org.jcb.kafka.service.schema.SchemaRegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping(value = "/schemas", produces = MediaType.APPLICATION_JSON_VALUE)
@ResponseBody
public class SchemaController {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaController.class);

    @Autowired
    private SchemaRegistryService schemaRegistryService;

    @PostMapping()
    public ResponseEntity<?> createSchema(@RequestParam String resourceName, @RequestParam String subject) {
        ResponseEntity<?> result = null;
        try {
            schemaRegistryService.registerSchema(resourceName, subject);
        } catch (IOException | RestClientException | SchemaResourceNotFound e) {
            LOGGER.error("Unable to register schema with resource {} and subject {}.", resourceName, subject, e);
            result = ResponseEntity.badRequest().body("Unable to register schema. " + e.getMessage());
        }
        if (result == null) {
            result = ResponseEntity.ok().build();
        }
        return result;
    }

    @DeleteMapping()
    public ResponseEntity<?> deleteSchema(@RequestParam String subject) {
        ResponseEntity<?> result = null;
        try {
            schemaRegistryService.unregisterSchema(subject);
        } catch (IOException | RestClientException e) {
            LOGGER.error("Unable to unregister schema with subject {}.", subject, e);
            result = ResponseEntity.badRequest().body("Unable to unregister schema. " + e.getMessage());
        }
        if (result == null) {
            result = ResponseEntity.ok().build();
        }
        return result;
    }

}
