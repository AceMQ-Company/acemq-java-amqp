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

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.acemq.amqp.transport.Acknowledger;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.DeliveryListener;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.acemq.amqp.transport.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * A RabbitMQ connection presented as a {@link TransportConnection}.
 *
 * <p>Channels are an implementation detail and never escape this class. Publishing uses one
 * dedicated confirm-mode channel guarded by a lock, because an AMQP 0-9-1 channel is not
 * thread safe and sharing one unguarded is a classic source of protocol errors under load.
 * Each subscription gets its own channel, so a slow consumer cannot stall a publisher.
 */
final class RabbitMqConnection implements TransportConnection {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqConnection.class);

    private final Connection connection;
    private final ConnectionConfig config;
    private final Channel publishChannel;
    private final Object publishLock = new Object();
    private final List<Channel> consumerChannels = new CopyOnWriteArrayList<>();
    private final Map<String, String> returnedMessages = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    RabbitMqConnection(Connection connection, ConnectionConfig config) {
        this.connection = connection;
        this.config = config;
        try {
            this.publishChannel = connection.createChannel();
            if (config.publisherConfirms()) {
                this.publishChannel.confirmSelect();
            }
            // A returned message means the broker accepted it but nothing was bound to
            // receive it. The confirm still arrives, so the return has to be recorded here
            // and correlated by message id when the confirm lands.
            this.publishChannel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> {
                String id = properties == null ? null : properties.getMessageId();
                if (id != null) {
                    returnedMessages.put(id, replyCode + " " + replyText);
                }
                log.warn("message returned as unroutable: exchange={} routingKey={} reason={} {}",
                        exchange, routingKey, replyCode, replyText);
            });
        } catch (IOException e) {
            throw new TransportException("could not open the publishing channel", e);
        }
    }

    @Override
    public void declareExchange(String name, String type, boolean durable) {
        withChannel("declare exchange " + name, channel -> {
            channel.exchangeDeclare(name, type, durable);
            return null;
        });
    }

    @Override
    public void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments) {
        Map<String, Object> args = arguments == null ? new HashMap<>() : new HashMap<>(arguments);
        if (type == QueueType.QUORUM) {
            args.put("x-queue-type", "quorum");
        } else if (type == QueueType.STREAM) {
            args.put("x-queue-type", "stream");
        }
        // Quorum and stream queues must be durable; declaring otherwise fails at the broker
        // with a message that does not explain itself, so it is corrected here.
        boolean effectiveDurable = durable || type != QueueType.CLASSIC;

        withChannel("declare queue " + name, channel -> {
            channel.queueDeclare(name, effectiveDurable, false, false, args);
            return null;
        });
    }

    @Override
    public void bindQueue(String queue, String exchange, String routingKey) {
        withChannel("bind " + queue + " to " + exchange, channel -> {
            channel.queueBind(queue, exchange, routingKey);
            return null;
        });
    }

    @Override
    public void deleteQueue(String name) {
        withChannel("delete queue " + name, channel -> {
            channel.queueDelete(name);
            return null;
        });
    }

    @Override
    public ConfirmResult send(OutboundMessage message) {
        AMQP.BasicProperties properties = properties(message);
        long startedAt = System.nanoTime();

        synchronized (publishLock) {
            try {
                if (message.messageId() != null) {
                    returnedMessages.remove(message.messageId());
                }
                publishChannel.basicPublish(
                        message.exchange(), message.routingKey(), message.mandatory(), properties, message.body());

                if (!config.publisherConfirms()) {
                    return ConfirmResult.confirmed(elapsedSince(startedAt));
                }

                boolean acked = publishChannel.waitForConfirms(config.confirmTimeout().toMillis());
                Duration latency = elapsedSince(startedAt);
                if (!acked) {
                    return ConfirmResult.failed(latency, "the broker rejected the message");
                }

                // waitForConfirms returning true only means the broker took the message. If
                // it was also returned as unroutable, that return has already arrived.
                String returned = message.messageId() == null ? null : returnedMessages.remove(message.messageId());
                return returned == null
                        ? ConfirmResult.confirmed(latency)
                        : ConfirmResult.unroutable(latency, returned);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransportException("interrupted while waiting for a publisher confirm", e);
            } catch (IOException | ShutdownSignalException e) {
                throw new TransportException("publish failed: " + message, e);
            } catch (java.util.concurrent.TimeoutException e) {
                throw new TransportException(
                        "no publisher confirm within " + config.confirmTimeout() + " for " + message, e);
            }
        }
    }

    @Override
    public Subscription subscribe(String queue, int prefetch, DeliveryListener listener) {
        if (prefetch < 1) {
            throw new IllegalArgumentException(
                    "prefetch must be at least 1, was " + prefetch + ". Unbounded prefetch is the fastest way to run"
                            + " a consumer out of memory, so it cannot be requested by accident.");
        }
        try {
            Channel channel = connection.createChannel();
            channel.basicQos(prefetch);
            consumerChannels.add(channel);

            String consumerTag = channel.basicConsume(queue, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(
                        String tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    InboundDelivery delivery = new InboundDelivery(
                            queue,
                            envelope.getExchange(),
                            envelope.getRoutingKey(),
                            body,
                            portableHeaders(properties.getHeaders()),
                            properties.getMessageId(),
                            properties.getContentType(),
                            envelope.isRedeliver());
                    listener.onDelivery(delivery, new ChannelAcknowledger(channel, envelope.getDeliveryTag()));
                }
            });
            return new ChannelSubscription(queue, channel, consumerTag, consumerChannels);
        } catch (IOException e) {
            throw new TransportException("could not consume queue " + queue, e);
        }
    }

    @Override
    public boolean isOpen() {
        return !closed.get() && connection.isOpen();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Channel channel : consumerChannels) {
            closeQuietly(channel);
        }
        consumerChannels.clear();
        closeQuietly(publishChannel);
        try {
            connection.close();
        } catch (IOException e) {
            log.debug("ignoring error while closing the connection", e);
        }
    }

    private AMQP.BasicProperties properties(OutboundMessage message) {
        AMQP.BasicProperties base = message.persistent() ? MessageProperties.PERSISTENT_BASIC : MessageProperties.BASIC;
        return base.builder()
                .headers(message.headers())
                .messageId(message.messageId())
                .contentType(message.contentType())
                .build();
    }

    private static Duration elapsedSince(long startedAtNanos) {
        return Duration.ofNanos(System.nanoTime() - startedAtNanos);
    }

    /**
     * Converts RabbitMQ's header types into the portable ones the SPI promises.
     *
     * <p>The client returns text headers as {@code LongString}, which is not a
     * {@code CharSequence} and does not equal the {@code String} that was published. Left
     * alone it reaches application code and makes an ordinary header comparison fail for
     * reasons that have nothing to do with the application. Nested lists and maps are
     * converted too, since a header value may contain them.
     */
    private static Map<String, Object> portableHeaders(Map<String, Object> headers) {
        if (headers == null) {
            return null;
        }
        Map<String, Object> portable = new HashMap<>(headers.size());
        headers.forEach((name, value) -> portable.put(name, portableValue(value)));
        return portable;
    }

    private static Object portableValue(Object value) {
        if (value instanceof com.rabbitmq.client.LongString) {
            return value.toString();
        }
        if (value instanceof List) {
            List<Object> converted = new java.util.ArrayList<>(((List<?>) value).size());
            for (Object element : (List<?>) value) {
                converted.add(portableValue(element));
            }
            return converted;
        }
        if (value instanceof Map) {
            Map<String, Object> converted = new HashMap<>();
            ((Map<?, ?>) value).forEach((key, nested) -> converted.put(String.valueOf(key), portableValue(nested)));
            return converted;
        }
        return value;
    }

    /**
     * Runs a one-off administrative operation on its own channel.
     *
     * <p>A failed declaration kills the channel it ran on. Using a throwaway channel keeps
     * that failure away from the publishing channel, which would otherwise take every
     * in-flight publish down with it.
     */
    private <T> T withChannel(String description, ChannelOperation<T> operation) {
        try (Channel channel = connection.createChannel()) {
            return operation.run(channel);
        } catch (Exception e) {
            throw new TransportException("could not " + description, e);
        }
    }

    private static void closeQuietly(Channel channel) {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (Exception e) {
            log.debug("ignoring error while closing a channel", e);
        }
    }

    @FunctionalInterface
    private interface ChannelOperation<T> {
        T run(Channel channel) throws Exception;
    }

    /** Settles one delivery, refusing to settle it twice. */
    private static final class ChannelAcknowledger implements Acknowledger {

        private final Channel channel;
        private final long deliveryTag;
        private final AtomicBoolean settled = new AtomicBoolean();

        ChannelAcknowledger(Channel channel, long deliveryTag) {
            this.channel = channel;
            this.deliveryTag = deliveryTag;
        }

        @Override
        public void accept() {
            if (settled.compareAndSet(false, true)) {
                try {
                    channel.basicAck(deliveryTag, false);
                } catch (IOException e) {
                    // The delivery will be redelivered when the channel closes. Failing loudly
                    // here would be worse: the message is not lost, only unsettled.
                    log.warn("could not acknowledge delivery {}", deliveryTag, e);
                }
            }
        }

        @Override
        public void reject(boolean requeue) {
            if (settled.compareAndSet(false, true)) {
                try {
                    channel.basicNack(deliveryTag, false, requeue);
                } catch (IOException e) {
                    log.warn("could not reject delivery {}", deliveryTag, e);
                }
            }
        }
    }

    /** A consumer registration tied to its own channel. */
    private static final class ChannelSubscription implements Subscription {

        private final String queue;
        private final Channel channel;
        private final String consumerTag;
        private final List<Channel> registry;
        private final AtomicBoolean active = new AtomicBoolean(true);

        ChannelSubscription(String queue, Channel channel, String consumerTag, List<Channel> registry) {
            this.queue = queue;
            this.channel = channel;
            this.consumerTag = consumerTag;
            this.registry = registry;
        }

        @Override
        public String queue() {
            return queue;
        }

        @Override
        public boolean isActive() {
            return active.get() && channel.isOpen();
        }

        @Override
        public void close() {
            if (!active.compareAndSet(true, false)) {
                return;
            }
            try {
                if (channel.isOpen()) {
                    // Cancelling before closing lets deliveries already dispatched finish
                    // settling, rather than turning a clean shutdown into redeliveries.
                    channel.basicCancel(consumerTag);
                }
            } catch (Exception e) {
                log.debug("ignoring error while cancelling consumer {}", consumerTag, e);
            }
            registry.remove(channel);
            closeQuietly(channel);
        }
    }
}
