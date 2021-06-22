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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Tags({"pulsar"})
@CapabilityDescription("The Pulsar Producer Pre-Processor allows nifi to stream data to Apache Pulsar.")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute="msg.count", description = "The number of messages that were sent to Kafka for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")})
public class PulsarProducerProcessor extends AbstractProcessor {

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

        /**List<PropertyDescriptor> constantValues = Arrays.stream(PulsarPropertiesUtils.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .map(field -> {
                    try {
                        return (PropertyDescriptor) field.get(PulsarPropertiesUtils.class);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());**/
        descriptors.add(PulsarPropertiesUtils.serviceUrl);
        descriptors.add(PulsarPropertiesUtils.useTls);
        descriptors.add(PulsarPropertiesUtils.tlsAllowInsecureConnection);
        descriptors.add(PulsarPropertiesUtils.tlsHostnameVerificationEnable);
        descriptors.add(PulsarPropertiesUtils.authPluginClassName);
        descriptors.add(PulsarPropertiesUtils.topicName);
        descriptors.add(PulsarPropertiesUtils.sendTimeoutMs);
        descriptors.add(PulsarPropertiesUtils.blockIfQueueFull);
        descriptors.add(PulsarPropertiesUtils.batchingMaxPublishDelayMicros);
        descriptors.add(PulsarPropertiesUtils.maxPendingMessages);
        descriptors.add(PulsarPropertiesUtils.batchingMaxMessages);
        descriptors.add(PulsarPropertiesUtils.batchingEnabled);
        descriptors.add(PulsarPropertiesUtils.compressionType);
        descriptors.add(PulsarPropertiesUtils.asyncEnabled);

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
        NifiPulsarProducer.initializeProducer(
            PulsarPropertiesUtils.getClientSettings(context),
            PulsarPropertiesUtils.getProducerSettings(context)
        );
    }

    @OnUnscheduled
    public void onUnscheduled(){
        NifiPulsarProducer.closeAll();
    }

    @OnStopped
    public void onStopped(){
        onUnscheduled();
    }

    protected static final String MSG_COUNT = "msg.count";

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 500));
        if (flowFiles.isEmpty() ) {
            return;
        }
        else{
            final Iterator<FlowFile> its=  flowFiles.iterator();
            while(its.hasNext()) {
                final FlowFile flowFile = its.next();
                final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                        .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;
                final NifiPulsarPublisher pulsarPublisher = new NifiPulsarPublisher(new InFlightMessageTracker(getLogger()), context);
                final Boolean asyncEnabled = Boolean.valueOf(context.getProperty("asyncEnabled").getValue());
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream inputStream) {
                        if(demarcatorBytes != null) {
                            pulsarPublisher.publish(flowFile, demarcatorBytes, inputStream, asyncEnabled);
                        }
                        else {
                            pulsarPublisher.publish(flowFile, inputStream, asyncEnabled);
                        }
                    }
                });

                if (pulsarPublisher.getTracker().isFailed(flowFile)) {
                    getLogger().info("Failed to send FlowFile to pulsar; transferring to failure");
                    session.transfer(flowFiles, REL_FAILURE);
                    return;
                }
                else{
                    FlowFile success = session.putAttribute(flowFile, MSG_COUNT, String.valueOf(pulsarPublisher.getTracker().getSentCount(flowFile)));
                    session.transfer(success, REL_SUCCESS);
                }
            }
        }
    }
}
