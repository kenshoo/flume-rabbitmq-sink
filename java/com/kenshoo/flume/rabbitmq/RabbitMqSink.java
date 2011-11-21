package com.kenshoo.flume.rabbitmq;

import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A flume sink implementation for routing messages into rabbitmq queues.
 * Pre condition - the incoming event has a "host" header containing the rabbitmq queue name.
 */
public class RabbitMqSink extends EventSink.Base {
    private QueuePublisher rabbitMqProducer;
    private String queueDomain;

    public RabbitMqSink(QueuePublisher rabbitMqProducer) {
        this(rabbitMqProducer,"");
    }

    public RabbitMqSink(QueuePublisher publisher, String queueDomain) {
        this.rabbitMqProducer = publisher;
        this.queueDomain = queueDomain;
    }

    @Override
    public void open() throws IOException {
        rabbitMqProducer.open();
    }

    @Override
    public void append(Event e) throws IOException {
        String queueName = extractTragetQueueName(e);
        if (domainWasSpecified())
            queueName = appendDomainName(queueName);
        rabbitMqProducer.publish(queueName,e.getBody());
    }

    private String appendDomainName(String queueName) {
        return queueDomain + "." + queueName;
    }

    private boolean domainWasSpecified() {
        return !"".equals(queueDomain);
    }

    private String extractTragetQueueName(Event e) {
        byte[] hostAttr = e.get("host");
        if (hostAttr == null)
            throw new IllegalStateException("Event is missing host attribute:" + e);
        return new String(hostAttr);
    }

    @Override
    public void close() throws IOException {
        rabbitMqProducer.close();
    }

    private static SinkBuilder builder() {
        return new RabbitMqSinkBuilder();
    }

  /**
   * Flume is looking for this method when creating an instance of the sink
   */
  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("rabbitMqSink", builder()));
    return builders;
  }

}
