package com.kenshoo.flume.rabbitmq;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: sagyr
 * Date: 8/2/11
 * Time: 5:54 PM
 * To change this template use File | Settings | File Templates.
 */
public interface QueuePublisher {
    void open() throws IOException;

    void close() throws IOException;

    void publish(String queueName, byte[] msg) throws IOException;
}
