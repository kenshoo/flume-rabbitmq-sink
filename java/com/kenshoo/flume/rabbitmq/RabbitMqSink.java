/*
* Copyright 2011 Kenshoo.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/  
package com.kenshoo.flume.rabbitmq;

import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A flume sink implementation for routing messages into rabbitmq queues.
 * Pre condition - the incoming event has a "host" header containing the rabbitmq queue name.
 */
public class RabbitMqSink extends EventSink.Base {

    private QueuePublisher rabbitMqProducer;
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMqSink.class);

    public RabbitMqSink(QueuePublisher publisher) {
        this.rabbitMqProducer = publisher;
    }

    @Override
    public void open() throws IOException {
        rabbitMqProducer.open();
    }

    @Override
    public void append(Event e) throws IOException {
        LOG.debug("received message: {}", e);
        String queueName = extractTragetQueueName(e);
        rabbitMqProducer.publish(queueName,e.getBody());
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
