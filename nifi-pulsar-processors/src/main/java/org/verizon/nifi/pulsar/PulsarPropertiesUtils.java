package org.verizon.nifi.pulsar;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.HashMap;
import java.util.Map;

public class PulsarPropertiesUtils {

    /**
     * Pulsar client configuration settings
     */

    public static Map<String, Object> getClientSettings(ProcessContext context){
        Map<String, Object> properties = new HashMap<>();
        properties.put("serviceUrl", context.getProperty("serviceUrl").getValue());
        properties.put("useTls", Boolean.valueOf(context.getProperty("useTls").getValue()));
        properties.put("tlsAllowInsecureConnection", context.getProperty("tlsAllowInsecureConnection").getValue());
        properties.put("tlsHostnameVerificationEnable", context.getProperty("tlsHostnameVerificationEnable").getValue());
        properties.put("authPluginClassName", context.getProperty("authPluginClassName").getValue());
        properties.put("authParams", context.getProperty("authParams").getValue());
        return properties;
    }

    public static final PropertyDescriptor serviceUrl = new PropertyDescriptor
            .Builder()
            .name("serviceUrl")
            .displayName("serviceUrl")
            .description("Service URL provider for Pulsar service")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor useTls = new PropertyDescriptor.Builder()
            .name("useTls")
            .displayName("TLS Key")
            .description("TLS Key for authentication against the pulsar broker")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor tlsAllowInsecureConnection = new PropertyDescriptor.Builder()
            .name("tlsAllowInsecureConnection")
            .displayName("tlsAllowInsecureConnection")
            .description("Whether the Pulsar client accepts untrusted TLS certificate from broker")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor tlsHostnameVerificationEnable = new PropertyDescriptor.Builder()
            .name("tlsHostnameVerificationEnable")
            .displayName("tlsHostnameVerificationEnable")
            .description("Whether to enable TLS hostname verification")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final PropertyDescriptor authPluginClassName = new PropertyDescriptor.Builder()
            .name("authPluginClassName")
            .displayName("authPluginClassName")
            .description("Name of the authentication plugin")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor authParams = new PropertyDescriptor.Builder()
            .name("authParams")
            .displayName("authParams")
            .description("String represents parameters for the authentication plugin Example: key1:val1,key2:val2")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Pulsar Producer Configuration Settings
     */

    public static Map<String, Object> getProducerSettings(ProcessContext context){
        Map<String, Object> properties = new HashMap<>();
        properties.put("topicName", context.getProperty("topicName").getValue());
        properties.put("sendTimeoutMs", context.getProperty("sendTimeoutMs").getValue());
        properties.put("blockIfQueueFull", Boolean.valueOf(context.getProperty("blockIfQueueFull").getValue()));
        properties.put("batchingMaxPublishDelayMicros", context.getProperty("batchingMaxPublishDelayMicros").getValue());
        properties.put("maxPendingMessages", context.getProperty("maxPendingMessages").getValue());
        properties.put("batchingMaxMessages", context.getProperty("batchingMaxMessages").getValue());
        properties.put("batchingEnabled", Boolean.valueOf(context.getProperty("batchingEnabled").getValue()));
        properties.put("compressionType", context.getProperty("compressionType").getValue());
        return properties;
    }

    public static final PropertyDescriptor topicName = new PropertyDescriptor
            .Builder()
            .name("topicName")
            .displayName("topicName")
            .description("Topic name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor sendTimeoutMs = new PropertyDescriptor.Builder()
            .name("sendTimeoutMs")
            .displayName("sendTimeoutMs")
            .description("If a message is not acknowledged by a server before the sendTimeout expires, an error occurs.")
            .required(false)
            .defaultValue("30000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor blockIfQueueFull = new PropertyDescriptor.Builder()
            .name("blockIfQueueFull")
            .displayName("blockIfQueueFull")
            .description("If it is set to true, when the outgoing message queue is full, the Send and SendAsync methods of producer block, rather than failing and throwing errors.\n" +
                    "\n" +
                    "If it is set to false, when the outgoing message queue is full, the Send and SendAsync methods of producer fail and ProducerQueueIsFullError exceptions occur.\n" +
                    "\n" +
                    "The MaxPendingMessages parameter determines the size of the outgoing message queue.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor batchingMaxPublishDelayMicros = new PropertyDescriptor.Builder()
            .name("batchingMaxPublishDelayMicros")
            .displayName("batchingMaxPublishDelayMicros")
            .description("Batching time period of sending messages.")
            .required(false)
            .defaultValue("1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor maxPendingMessages = new PropertyDescriptor.Builder()
            .name("maxPendingMessages")
            .displayName("maxPendingMessages")
            .description("The maximum size of a queue holding pending messages.")
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor batchingMaxMessages = new PropertyDescriptor.Builder()
            .name("batchingMaxMessages")
            .displayName("batchingMaxMessages")
            .description("The maximum number of messages permitted in a batch.")
            .required(false)
            .defaultValue("1000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor batchingEnabled = new PropertyDescriptor.Builder()
            .name("batchingEnabled")
            .displayName("batchingEnabled")
            .description("Enable batching of messages.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor compressionType = new PropertyDescriptor.Builder()
            .name("compressionType")
            .displayName("compressionType")
            .description("Message data compression type used by a producer.\n" +
                    "\n" +
                    "Available options:\n" +
                    "LZ4\n" +
                    "ZLIB\n" +
                    "ZSTD\n" +
                    "SNAPPY")
            .required(false)
            .defaultValue("LZ4")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACK_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ACK_TIMEOUT")
            .displayName("Acknowledgment Timeout")
            .description("Set the timeout for unacked messages. Messages that are not acknowledged within the "
                    + "configured timeout will be replayed. This value needs to be greater than 10 seconds.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .required(false)
            .build();

    public static final PropertyDescriptor subscriptionName = new PropertyDescriptor.Builder()
            .name("subscriptionName")
            .displayName("subscriptionName")
            .description("Subscription name for the consumer")
            .required(true)
            .defaultValue("test-subscriber")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor asyncEnabled = new PropertyDescriptor.Builder()
            .name("asyncEnabled")
            .displayName("asyncEnabled")
            .description("If Enabled will async send messages to the pulsar broker")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

}
