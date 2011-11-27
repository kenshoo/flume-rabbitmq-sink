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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.core.EventSink;

/**
* Created by IntelliJ IDEA.
* User: sagyr
* Date: 8/2/11
* Time: 7:23 PM
* To change this template use File | Settings | File Templates.
*/
class RabbitMqSinkBuilder extends SinkFactory.SinkBuilder {
    @Override
    public EventSink build(Context context, String... args) {
        if (!(args.length >= 1) )
		throw new IllegalArgumentException("Missing arguments: queue host,[queue-domain=\"\"],[username=\"guest\"],[password=\"guest\"]");
        String publisher = args[0];
        String queueDomain = args.length >= 2 ? args[1] : "";
	String userName = args.length>=3? args[2]: "guest";
	String password = args.length>=4? args[3]: "guest";
        return new RabbitMqSink(new SimpleRabbitMqProducer(publisher, userName, password),queueDomain);
    }
}
