/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.verizon.nifi.pulsar;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pulsar.client.api.*;
import org.verizon.nifi.pulsar.consumer.PulsarClientLRUCache;
import org.verizon.nifi.pulsar.consumer.PulsarClientService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"pulsar"})
@CapabilityDescription("The Pulsar Consumer Pre-Processor allows nifi to stream data from Apache Pulsar to Apache Nifi.")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute="msg.count", description = "The number of messages that were sent to Kafka for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")})
public class PulsarConsumerProcessor extends AbstractProcessor {

    static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
            + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages.");

    static final AllowableValue CONSUME = new AllowableValue(ConsumerCryptoFailureAction.CONSUME.name(), "Consume",
            "Mark the message as consumed despite being unable to decrypt the contents");
    static final AllowableValue DISCARD = new AllowableValue(ConsumerCryptoFailureAction.DISCARD.name(), "Discard",
            "Discard the message and don't perform any addtional processing on the message");
    static final AllowableValue FAIL = new AllowableValue(ConsumerCryptoFailureAction.FAIL.name(), "Fail",
            "Report a failure condition, and then route the message contents to the FAILED relationship.");

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("MESSAGE_DEMARCATOR")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                    + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                    + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Pulsar message. "
                    + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_TYPE = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_TYPE")
            .displayName("Subscription Type")
            .description("Select the subscription type to be used when subscribing to the topic.")
            .required(true)
            .allowableValues(EXCLUSIVE, SHARED, FAILOVER)
            .defaultValue(SHARED.getValue())
            .build();

    public static final PropertyDescriptor MAX_ASYNC_REQUESTS = new PropertyDescriptor.Builder()
            .name("MAX_ASYNC_REQUESTS")
            .displayName("Maximum Async Requests")
            .description("The maximum number of outstanding asynchronous consumer requests for this processor. "
                    + "Each asynchronous call requires memory, so avoid setting this value to high.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor CONSUMER_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("CONSUMER_BATCH_SIZE")
            .displayName("Consumer Message Batch Size")
            .description("Set the maximum number of messages consumed at a time, and published to a single FlowFile. "
                    + "default: 1000. If set to a value greater than 1, messages within the FlowFile will be seperated "
                    + "by the Message Demarcator.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Pulsar.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Pulsar will be routed to this Relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();

        descriptors.add(PulsarPropertiesUtils.serviceUrl);
        descriptors.add(PulsarPropertiesUtils.useTls);
        descriptors.add(PulsarPropertiesUtils.tlsAllowInsecureConnection);
        descriptors.add(PulsarPropertiesUtils.tlsHostnameVerificationEnable);
        descriptors.add(PulsarPropertiesUtils.authPluginClassName);
        descriptors.add(PulsarPropertiesUtils.topicName);
        descriptors.add(PulsarPropertiesUtils.subscriptionName);
        descriptors.add(MESSAGE_DEMARCATOR);
        descriptors.add(PulsarPropertiesUtils.ACK_TIMEOUT);
        descriptors.add(SUBSCRIPTION_TYPE);
        descriptors.add(PulsarPropertiesUtils.asyncEnabled);
        descriptors.add(MAX_ASYNC_REQUESTS);
        descriptors.add(CONSUMER_BATCH_SIZE);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(PulsarPropertiesUtils.asyncEnabled).isSet()) {
            setConsumerPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger()));
            setConsumerService(new ExecutorCompletionService<>(getConsumerPool()));
            setAckPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger() + 1));
            setAckService(new ExecutorCompletionService<>(getAckPool()));
        }

        setPulsarClientService(context.getProperty(PulsarPropertiesUtils.serviceUrl).asControllerService(PulsarClientService.class));

    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
        /*
         * If we are running in asynchronous mode, then we need to stop all of the consumer threads that
         * are running in the ConsumerPool. After, we have stopped them, we need to wait a little bit
         * to ensure that all of the messages are properly acked, in order to prevent re-processing the
         * same messages in the event of a shutdown and restart of the processor since the un-acked
         * messages would be replayed on startup.
         */
        if (context.getProperty(PulsarPropertiesUtils.asyncEnabled).isSet() && context.getProperty(PulsarPropertiesUtils.asyncEnabled).asBoolean()) {
            try {
                getConsumerPool().shutdown();
                getAckPool().shutdown();

                // Allow some time for the acks to be sent back to the Broker.
                getConsumerPool().awaitTermination(10, TimeUnit.SECONDS);
                getAckPool().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getLogger().error("Unable to stop all the Pulsar Consumers", e);
            }
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context){
        shutDown(context);
        getConsumers().clear();
    }

    protected static final String MSG_COUNT = "msg.count";

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            Consumer<byte[]> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (consumer == null) {
                context.yield();
                return;
            }

            if (context.getProperty(PulsarPropertiesUtils.asyncEnabled).asBoolean()) {
                consumeAsync(consumer, context, session);
                handleAsync(consumer, context, session);
            } else {
                consume(consumer, context, session);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

    private void handleAsync(final Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) {
        try {
            Future<List<Message<byte[]>>> done = getConsumerService().poll(5, TimeUnit.SECONDS);

            if (done != null) {

                final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                        .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;

                List<Message<byte[]>> messages = done.get();

                if (CollectionUtils.isNotEmpty(messages)) {
                    FlowFile flowFile = session.create();
                    OutputStream out = session.write(flowFile);
                    AtomicInteger msgCount = new AtomicInteger(0);

                    messages.forEach(msg -> {
                        try {
                            out.write(msg.getValue());
                            out.write(demarcatorBytes);
                            msgCount.getAndIncrement();
                        } catch (final IOException ioEx) {
                            session.rollback();
                            return;
                        }
                    });

                    IOUtils.closeQuietly(out);

                    session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                    session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commit();
                }
                // Acknowledge consuming the message
                getAckService().submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        return consumer.acknowledgeCumulativeAsync(messages.get(messages.size()-1)).get();
                    }
                });
            }
        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        }
    }

    private void consume(Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;

            FlowFile flowFile = session.create();
            OutputStream out = session.write(flowFile);
            Message<byte[]> msg = null;
            Message<byte[]> lastMsg = null;
            AtomicInteger msgCount = new AtomicInteger(0);
            AtomicInteger loopCounter = new AtomicInteger(0);

            while (((msg = consumer.receive(0, TimeUnit.SECONDS)) != null) && loopCounter.get() < maxMessages) {
                try {

                    lastMsg = msg;
                    loopCounter.incrementAndGet();

                    // Skip empty messages, as they cause NPE's when we write them to the OutputStream
                    if (msg.getValue() == null || msg.getValue().length < 1) {
                        continue;
                    }
                    out.write(msg.getValue());
                    out.write(demarcatorBytes);
                    msgCount.getAndIncrement();

                } catch (final IOException ioEx) {
                    session.rollback();
                    return;
                }
            }

            IOUtils.closeQuietly(out);

            if (lastMsg != null)  {
                consumer.acknowledgeCumulative(lastMsg);
            }

            if (msgCount.get() < 1) {
                session.remove(flowFile);
                session.commit();
            } else {
                session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                        new Object[]{flowFile, msgCount.toString()});
            }

        } catch (PulsarClientException e) {
            context.yield();
            session.rollback();
        }
    }

    protected String getConsumerId(final ProcessContext context, FlowFile flowFile) {
        if (context == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();

        if (context.getProperty(PulsarPropertiesUtils.topicName).isSet()) {
            sb.append(context.getProperty(PulsarPropertiesUtils.topicName).evaluateAttributeExpressions(flowFile).getValue());
        }

        sb.append("-").append(context.getProperty(PulsarPropertiesUtils.subscriptionName).getValue());

        if (context.getProperty(PulsarPropertiesUtils.subscriptionName).isSet()) {
            sb.append("-").append(context.getProperty(PulsarPropertiesUtils.subscriptionName).getValue());
        }
        return sb.toString();
    }

    protected void consumeAsync(final Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            getConsumerService().submit(() -> {
                List<Message<byte[]>> messages = new LinkedList<>();
                Message<byte[]> msg = null;
                AtomicInteger msgCount = new AtomicInteger(0);

                while (((msg = consumer.receive(0, TimeUnit.SECONDS)) != null) && msgCount.get() < maxMessages) {
                    messages.add(msg);
                    msgCount.incrementAndGet();
                }
                return messages;
            });
        } catch (final RejectedExecutionException ex) {
            getLogger().error("Unable to consume any more Pulsar messages", ex);
            context.yield();
        }
    }

    protected synchronized Consumer<byte[]> getConsumer(ProcessContext context, String topic) throws PulsarClientException {

        Consumer<byte[]> consumer = getConsumers().get(topic);

        if (consumer != null && consumer.isConnected()) {
            return consumer;
        }

        // Create a new consumer and validate that it is connected before returning it.
        consumer = getConsumerBuilder(context).subscribe();
        if (consumer != null && consumer.isConnected()) {
            getConsumers().put(topic, consumer);
        }

        return (consumer != null && consumer.isConnected()) ? consumer : null;
    }

    protected synchronized ConsumerBuilder<byte[]> getConsumerBuilder(ProcessContext context) throws PulsarClientException {

        ConsumerBuilder<byte[]> builder = getPulsarClientService().getPulsarClient().newConsumer();

        if (context.getProperty(PulsarPropertiesUtils.topicName).isSet()) {
            builder = builder.topic(Arrays.stream(context.getProperty(PulsarPropertiesUtils.topicName).evaluateAttributeExpressions().getValue().split("[, ]"))
                    .map(String::trim).toArray(String[]::new));
        }

        if (context.getProperty(PulsarPropertiesUtils.subscriptionName).isSet()) {
            builder = builder.consumerName(context.getProperty(PulsarPropertiesUtils.subscriptionName).getValue());
        }

        return builder.subscriptionName(context.getProperty(PulsarPropertiesUtils.subscriptionName).getValue())
                .ackTimeout(context.getProperty(PulsarPropertiesUtils.ACK_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                .subscriptionType(SubscriptionType.valueOf(context.getProperty(SUBSCRIPTION_TYPE).getValue()));
    }

    private PulsarClientService pulsarClientService;
    private PulsarClientLRUCache<String, Consumer<byte[]>> consumers;
    private ExecutorService consumerPool;
    private ExecutorCompletionService<List<Message<byte[]>>> consumerService;
    private ExecutorService ackPool;
    private ExecutorCompletionService<Object> ackService;

    protected synchronized ExecutorService getConsumerPool() {
        return consumerPool;
    }

    protected synchronized void setConsumerPool(ExecutorService pool) {
        this.consumerPool = pool;
    }

    protected synchronized ExecutorCompletionService<List<Message<byte[]>>> getConsumerService() {
        return consumerService;
    }

    protected synchronized void setConsumerService(ExecutorCompletionService<List<Message<byte[]>>> service) {
        this.consumerService = service;
    }

    protected synchronized ExecutorService getAckPool() {
        return ackPool;
    }

    protected synchronized void setAckPool(ExecutorService pool) {
        this.ackPool = pool;
    }

    protected synchronized ExecutorCompletionService<Object> getAckService() {
        return ackService;
    }

    protected synchronized void setAckService(ExecutorCompletionService<Object> ackService) {
        this.ackService = ackService;
    }

    protected synchronized PulsarClientService getPulsarClientService() {
        return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
        this.pulsarClientService = pulsarClientService;
    }

    protected synchronized PulsarClientLRUCache<String, Consumer<byte[]>> getConsumers() {
        if (consumers == null) {
            consumers = new PulsarClientLRUCache<String, Consumer<byte[]>>(20);
        }
        return consumers;
    }

    protected void setConsumers(PulsarClientLRUCache<String, Consumer<byte[]>> consumers) {
        this.consumers = consumers;
    }
}
