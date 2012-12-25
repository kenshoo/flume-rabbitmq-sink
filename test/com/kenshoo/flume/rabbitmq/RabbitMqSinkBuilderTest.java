package com.kenshoo.flume.rabbitmq;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.rabbitmq.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created with IntelliJ IDEA.
 * User: liorh
 * Date: 10/22/12
 * Time: 12:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class RabbitMqSinkBuilderTest {
    private String host = System.getProperty("RABBIT_HOST", "localhost");
    private String username = System.getProperty("RABBIT_USERNAME","guest");
    private String password = System.getProperty("RABBIT_PASSWORD","guest");
    private String vhost = System.getProperty("RABBIT_VHOST","/");
    private Connection connection;

    private Channel channel;
    private String ksNumber = "1001";

    @Before
    public void setupQueue() throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(vhost);
        connection = connectionFactory.newConnection(Address.parseAddresses(host));
        channel = connection.createChannel();
        channel.exchangeDeclare(SimpleRabbitMqProducer.EXCHANGE_NAME, "direct", true);

    }

    @Test
    public void verifyMessageRoutedToQueue() throws IOException, InterruptedException {
        RabbitMqSinkBuilder builder = new RabbitMqSinkBuilder();
        Context context=null;
        EventSink sink = builder.build(context, host, username, password, vhost);
        try{
            sink.open();
            final String msgBody = "message body";
            Event event = new EventStub(msgBody);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, SimpleRabbitMqProducer.EXCHANGE_NAME,ksNumber);
            event.set("host", ksNumber.getBytes());
            sink.append(event);
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            assertThat(new String(delivery.getBody()), is(equalTo(msgBody)));
        }
        finally{
            sink.close();
        }

    }

    @Test
    public void verifyMessageNotReceived() throws IOException, InterruptedException {
        RabbitMqSinkBuilder builder = new RabbitMqSinkBuilder();
        Context context=null;
        EventSink sink = builder.build(context, host, username, password, vhost);
        try{
            sink.open();
            final String msgBody = "message body";
            Event event = new EventStub(msgBody);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, SimpleRabbitMqProducer.EXCHANGE_NAME,"nonexistent-queue");
            event.set("host", ksNumber.getBytes());
            sink.append(event);
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(100);
            assertNull(delivery);
        }
        finally{
            sink.close();
        }

    }


    @After
    public void disconnect() throws IOException {
        channel.exchangeDelete(SimpleRabbitMqProducer.EXCHANGE_NAME, false);
        channel.close();
        connection.close();
    }
}
