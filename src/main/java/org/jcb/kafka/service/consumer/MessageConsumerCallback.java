/*
 * Copyright (c) 2022.
 * Juan Barraza
 */

package org.jcb.kafka.service.consumer;

public interface MessageConsumerCallback<T> {

    void call(T data);

}
