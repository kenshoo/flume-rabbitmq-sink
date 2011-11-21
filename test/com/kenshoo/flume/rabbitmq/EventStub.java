package com.kenshoo.flume.rabbitmq;

import com.cloudera.flume.core.EventBaseImpl;

/**
* Created by IntelliJ IDEA.
* User: sagyr
* Date: 8/2/11
* Time: 7:18 PM
* To change this template use File | Settings | File Templates.
*/
class EventStub extends EventBaseImpl {
    private final String msgBody;

    public EventStub(String msgBody) {
        this.msgBody = msgBody;
    }

    @Override
    public byte[] getBody() {
        return msgBody.getBytes();
    }

    @Override
    public Priority getPriority() {
        return Priority.INFO;
    }

    @Override
    public long getTimestamp() {
        return 0;
    }

    @Override
    public long getNanos() {
        return 0;
    }

    @Override
    public String getHost() {
        return "SOME_HOST";
    }
}
