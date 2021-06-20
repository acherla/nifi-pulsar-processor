package org.verizon.nifi.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class NifiPulsarProducer {

    private static List<CompletableFuture<Producer<byte[]>>> lease = Collections.synchronizedList(new ArrayList<>());

    public static synchronized void initializeProducer(
            final Map<String, Object> pulsarClientProperties,
            final Map<String, Object> pulsarProducerProperties
    ) {
        CompletableFuture<Producer<byte[]>> producer = null;
        try {
            producer = PulsarClient.builder()
                    .loadConf(pulsarClientProperties)
                    .build()
                    .newProducer(Schema.BYTES)
                    .loadConf(pulsarProducerProperties)
                    .createAsync();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        lease.add(producer);
    }

    public static synchronized CompletableFuture<Producer<byte[]>> getLease(){
        Optional<CompletableFuture<Producer<byte[]>>> producer = lease.stream().findFirst();
        if(producer.isPresent()){
            return producer.get();
        }
        else{
            throw new RuntimeException("Unable to fetch any existing leases for pulsar!");
        }
    }

    public static synchronized void closeAll(){
        for(CompletableFuture<Producer<byte[]>> producerCompletableFuture : lease){
            try {
                producerCompletableFuture.get().flush();
                producerCompletableFuture.get().close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
