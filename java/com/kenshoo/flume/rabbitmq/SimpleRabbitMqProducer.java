package com.kenshoo.flume.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: sagyr
 * Date: 7/5/11
 * Time: 7:12 PM
 */
public class SimpleRabbitMqProducer implements QueuePublisher {
    private ConnectionFactory factory;
    private Connection conn;
    private Channel channel;

    public SimpleRabbitMqProducer(String host, String userName, String password) {
        factory = new ConnectionFactory();
        factory.setUsername(userName);
        factory.setPassword(password);
        factory.setVirtualHost("/");
        factory.setHost(host);
        factory.setPort(5672);
    }

    public void open() throws IOException {
        conn = factory.newConnection();
        channel = conn.createChannel();
    }

    public void close() throws IOException {
        channel.close();
        conn.close();
    }

    public void publish(String queueName, byte[] msg) throws IOException {
        channel.queueDeclare(queueName, false, false, false, null);
        channel.basicPublish("", queueName, MessageProperties.TEXT_PLAIN, msg);
    }
}
