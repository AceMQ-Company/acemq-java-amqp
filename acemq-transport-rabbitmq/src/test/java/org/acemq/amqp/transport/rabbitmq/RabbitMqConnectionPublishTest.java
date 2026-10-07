/*
 * Copyright 2026 AceMQ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acemq.amqp.transport.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.OutboundMessage;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;

/**
 * What a synchronous publish reports, against a client that returns every message as
 * unroutable. No broker: the client is a stand-in, so this is about the bookkeeping alone.
 */
class RabbitMqConnectionPublishTest {

    /** One channel of the stand-in client: returns every publish, then confirms it if asked. */
    private static final class FakeChannel {
        final List<ReturnListener> returns = new ArrayList<>();
        boolean confirming;
        int published;

        Channel proxy() {
            return (Channel) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Channel.class},
                    (self, method, args) -> {
                        switch (method.getName()) {
                            case "confirmSelect" :
                                confirming = true;
                                return null;
                            case "addReturnListener" :
                                returns.add((ReturnListener) args[0]);
                                return null;
                            case "basicPublish" :
                                published++;
                                AMQP.BasicProperties props = (AMQP.BasicProperties) args[args.length - 2];
                                for (ReturnListener listener : returns) {
                                    listener.handleReturn(312, "NO_ROUTE", (String) args[0], (String) args[1], props,
                                            (byte[]) args[args.length - 1]);
                                }
                                return null;
                            case "waitForConfirms" :
                                if (!confirming) {
                                    throw new IllegalStateException("not in confirm mode");
                                }
                                return true;
                            case "isOpen" :
                                return true;
                            default :
                                return method.getReturnType() == boolean.class ? false : null;
                        }
                    });
        }
    }

    private final List<FakeChannel> channels = new ArrayList<>();

    private RabbitMqConnection connect(ConnectionConfig config) {
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (self, method, args) -> {
                    if (method.getName().equals("createChannel")) {
                        FakeChannel channel = new FakeChannel();
                        channels.add(channel);
                        return channel.proxy();
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        return new RabbitMqConnection(connection, config, Executors.newSingleThreadExecutor());
    }

    private static OutboundMessage message() {
        return OutboundMessage.body(new byte[]{1}).exchange("").routingKey("nowhere").build();
    }

    @Test
    void a_return_for_a_message_without_an_id_is_still_reported() {
        // A foreign message, parked or replayed with no message id. Matching returns by id
        // reported it routed, and the delivery it was copied from was acknowledged.
        RabbitMqConnection connection = connect(ConnectionConfig.url("amqp://localhost").build());

        ConfirmResult result = connection.send(message());

        assertThat(result.isConfirmed()).isTrue();
        assertThat(result.isRouted()).isFalse();
    }

    @Test
    void without_confirms_the_librarys_own_publish_is_confirmed_on_a_channel_of_its_own() {
        RabbitMqConnection connection = connect(
                ConnectionConfig.url("amqp://localhost").withoutPublisherConfirms().build());

        ConfirmResult own = connection.send(OutboundMessage.body(new byte[]{1}).exchange("").routingKey("nowhere")
                .messageId("m-1").alwaysConfirmed().build());
        assertThat(own.isRouted()).isFalse();

        // The caller chose not to be told; its publish keeps that mode, on its own channel.
        ConfirmResult callers = connection.send(message());
        assertThat(callers.isRouted()).isTrue();

        assertThat(channels).hasSize(2);
        assertThat(channels.get(0).confirming).isFalse();
        assertThat(channels.get(0).published).isEqualTo(1);
        assertThat(channels.get(1).confirming).isTrue();
        assertThat(channels.get(1).published).isEqualTo(1);
    }

    @Test
    void with_confirms_there_is_only_one_publishing_channel() {
        connect(ConnectionConfig.url("amqp://localhost").build());
        assertThat(channels).hasSize(1);
        assertThat(channels.get(0).confirming).isTrue();
    }
}
