package com.kenshoo.flume.rabbitmq;


import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.core.EventBaseImpl;
import com.cloudera.util.Pair;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: sagyr
 * Date: 8/2/11
 * Time: 5:59 PM
 * To change this template use File | Settings | File Templates.
 */

@RunWith(JMock.class)
public class RabbitMqSinkTest {

    private static final String QUEUE_NAME = "queue.name";
    public static final String MSG_BODY = "The message body";
    private JUnit4Mockery context;
    private QueuePublisher publisher;
    private RabbitMqSink sink;
    private EventBaseImpl event;

    @Before
    public void setup() {
        context = new JUnit4Mockery();
        publisher = context.mock(QueuePublisher.class);
        sink = new RabbitMqSink(publisher);
        event = new EventStub(MSG_BODY);
        event.set("host",QUEUE_NAME.getBytes());
    }

    @Test
    public void publishesMessageBodyToCorrectQueue() throws IOException {
        context.checking(new Expectations(){{
            oneOf(publisher).publish(QUEUE_NAME,MSG_BODY.getBytes());
        }});
        sink.append(event);
    }

    @Test
    public void appendsPrefixToQueueNameIfGiven() throws IOException {
        sink = new RabbitMqSink(publisher,"queue.prefix");
        context.checking(new Expectations(){{
            oneOf(publisher).publish("queue.prefix." + QUEUE_NAME,MSG_BODY.getBytes());
        }});
        sink.append(event);
    }

    @Test(expected = IllegalStateException.class)
    public void throwsExceptionWhenHostHeaderIsMissingFromIncomingEvent() throws IOException {
        event = new EventStub("Event with missing host header");
        context.checking(new Expectations(){{
            never(publisher).publish(with(any(String.class)),with(any(byte[].class)));
        }});
        sink.append(event);
    }

    @Test
    public void registersSinkBuilderPlugin() {
        List<Pair<String, SinkFactory.SinkBuilder>> buildersList = RabbitMqSink.getSinkBuilders();
        Pair<String, SinkFactory.SinkBuilder> builderEntry = buildersList.get(0);
        assertEquals("rabbitMqSink", builderEntry.getLeft());
        assertTrue(builderEntry.getRight() instanceof RabbitMqSinkBuilder);
    }
}
