package com.github.robertzych.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDexcom {
    private static final String bootstrapServers = System.getenv("bootstrap.servers");
    private static final String groupId = System.getenv("groupId");
    private static final String topic = System.getenv("topic");
    static Logger logger = LoggerFactory.getLogger(ConsumerDexcom.class.getName());

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // subscribe consumer to the topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){
                logger.info("Offset: " + record.offset());
                logger.info("Value: " + record.value());

                // parse JSON from recorde into an Egv
                ObjectMapper objectMapper = new ObjectMapper();
                Egv egv = null;
                try {
                    egv = objectMapper.readValue(record.value(), Egv.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    logger.error("Failed to map json to egv", e);
                }
                if (egv != null) {
                    logger.info("System Time: " + egv.systemTime);
                    logger.info("EGV: " + egv.value);
                    // TODO: put to elastic /dexcom/egv/{id}
                }
            }
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        return new KafkaConsumer<>(properties);
    }
}
