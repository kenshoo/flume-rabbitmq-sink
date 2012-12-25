package com.kenshoo.flume.rabbitmq;

import org.junit.Test;

/**
 * User: lior
 * Date: 12/25/12
 * Time: 3:30 PM
 */
public class SimpleRabbitMqProducerTest {
    @Test
    public void avoidExceptionOnClose() throws Exception {
        SimpleRabbitMqProducer rabbitMqProducer = new SimpleRabbitMqProducer("nonExistent", "guest" ,"guest", "/");
        try{
            rabbitMqProducer.open();
        } catch (Exception ex){
            //should happen
        }
        rabbitMqProducer.close();
    }
}
