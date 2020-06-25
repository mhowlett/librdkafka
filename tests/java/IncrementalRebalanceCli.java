import java.io.IOException;
import java.io.PrintWriter;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.KafkaException;

import java.lang.Integer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.time.Duration;


public class IncrementalRebalanceCli {
    public static void main (String[] args) throws Exception {
        String testName = args[0];
        String brokerList = args[1];
        String topic1 = args[2];
        String topic2 = args[3];
        String group = args[4];

        if (!testName.equals("test1")) {
            throw new Exception("Unknown command: " + testName);
        }

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "java_incrreb_consumer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);

        List<String> topics = new ArrayList<>();
        topics.add(topic1);
        topics.add(topic2);
        consumer.subscribe(topics);

        long startTime = System.currentTimeMillis();
        long timeout_s = 300;

        try {
            boolean running = true;
            while (running) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                if (System.currentTimeMillis() - startTime > 1000 * timeout_s) {
                    // Ensure process exits eventually no matter what happens.
                    System.out.println("IncrementalRebalanceCli timed out");
                    running = false;
                }
                if (consumer.assignment().size() == 6) {
                    // librdkafka has unsubscribed from topic #2, exit cleanly.
                    running = false;
                }
            }
        } finally {
            consumer.close();
        }

        System.out.println("Java consumer process exiting");
    }
}
