# Flume RabbitMQ sink
A custom [Flume](https://github.com/cloudera/flume) sink that integrates
between flume and [rabbit-mq](http://www.rabbitmq.com/). 

## How it works
the rabbit-mq sink sends each event it receives to a queue, the queue name can
be determined using a parameter in the event's metadata map.
the rabbit-mq host, user and password can be configured as well.

## Usage
This project uses [gradle](http://www.gradle.org/) as build tool

1. Clone the repository
2. run "gradle build" from the project root dir
3. copy rabbit-sink-{ver}.jar from build/libs directory to flume master and
node classpath dir
4. add com.kenshoo.flume.rabbitmq.RabbitMqSink to flume-site.xml plugins
section on the master node.
5. (re)start master node and verify RabbitSink is in the plugins list
6. on the collector sink configuration, add rabbitsink('host','user','pass')

## License

This code is released under the Apache Public License 2.0.

