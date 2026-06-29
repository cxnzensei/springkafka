package dev.projects.springkafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(topics = "greetings", partitions = 1)
public class KafkaMessageTests {

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void sendsAndReceivesMessages() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafkaBroker, "test-group", true);
        try(Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("greetings"));

            // drain anything on the app already produced at startup
            KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(1));
            kafkaTemplate.send("greetings", "Hello from the test");
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "greetings", Duration.ofSeconds(5));

            assertEquals("Hello from the test", record.value());
        }
    }
}
