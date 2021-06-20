package org.verizon.nifi.pulsar;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import static org.verizon.nifi.pulsar.PulsarProducerProcessor.MESSAGE_DEMARCATOR;

public class NifiPulsarPublisher {

    private InFlightMessageTracker tracker;
    private ProcessContext context;

    public NifiPulsarPublisher(InFlightMessageTracker tracker, ComponentLog logger, ProcessContext context) {
        this.tracker = new InFlightMessageTracker(logger);
        this.context = context;
    }

    public NifiPulsarPublisher(InFlightMessageTracker tracker,
                               final ProcessContext context) {
        this.tracker = tracker;
        this.context = context;
    }

    /**
     * Publish the entire flowfile as a single message
     * @param flowFile
     * @param flowFileInputStream
     * @param asyncEnabled
     */
    void publish(final FlowFile flowFile, final InputStream flowFileInputStream, Boolean asyncEnabled){
        final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;

        if(flowFile.getSize() != 0) {
            try {
                byte[] messageContent;
                messageContent = new byte[(int) flowFile.getSize()];
                StreamUtils.fillBuffer(flowFileInputStream, messageContent);
                if(asyncEnabled) {
                    NifiPulsarProducer.getLease().get()
                            .newMessage(Schema.BYTES)
                            .value(messageContent)
                            .properties(flowFile.getAttributes())
                            .sendAsync()
                            .thenRunAsync(() -> {
                                tracker.incrementAcknowledgedCount(flowFile);
                            })
                            .exceptionally(ex -> {
                                tracker.fail(flowFile, new Exception(ex));
                                return null;
                            });
                }
                else{
                    NifiPulsarProducer.getLease().get()
                            .newMessage(Schema.BYTES)
                            .value(messageContent)
                            .properties(flowFile.getAttributes())
                            .send();
                    tracker.incrementSentCount(flowFile);
                }

            } catch (InterruptedException e) {
                tracker.fail(flowFile, e);
            } catch (ExecutionException e) {
                tracker.fail(flowFile, e);
            } catch (IOException e) {
                tracker.fail(flowFile, e);
            } catch(Exception e){
                tracker.fail(flowFile, e);
            }
        }
        else{
            tracker.trackEmpty(flowFile);
        }
    }

    public InFlightMessageTracker getTracker() {
        return tracker;
    }
}
