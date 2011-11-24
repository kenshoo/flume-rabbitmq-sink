# Flume RabbitMQ sink
A custom [Flume](https://github.com/cloudera/flume) sink that integrates
between flume and [RabbitMQ](http://www.rabbitmq.com/). 

## How it works
The sink sends each Flume event received to a RabbitMQ queue. The queue name is
determined by a parameter in the event's metadata map.
The RabbitMQ's host, user and password are all configurable as well.

## Usage
This project uses [gradle](http://www.gradle.org/) as its build tool.

Steps:
1. Clone the repository.
2. Run "gradle build" from the project's root dir.
3. Copy rabbit-sink-{ver}.jar from build/libs directory to the flume master and
node classpath dir.
4. Add com.kenshoo.flume.rabbitmq.RabbitMqSink to flume-site.xml plugins
section on the master node.
5. (Re)start the master node and verify that RabbitSink is part of the plugins list.
6. On the collector-sink's configuration, add rabbitsink('host','user','pass').

## License
This code is released under the Apache Public License 2.0.

