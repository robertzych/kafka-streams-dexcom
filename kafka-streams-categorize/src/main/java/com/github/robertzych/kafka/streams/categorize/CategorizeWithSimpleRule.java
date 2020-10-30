package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class CategorizeWithSimpleRule {

    public static void main(String[] args) {
        // create properties
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-categorize-simple");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        CategorizeWithSimpleRule categorizeWithSimpleRule = new CategorizeWithSimpleRule();

        KafkaStreams streams = new KafkaStreams(categorizeWithSimpleRule.createTopology(), config);

        // start our streams application
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(System.out::println);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // input topic
        KStream<String, JsonNode> egvs = builder.stream("egvs_topic",
                Consumed.with(Serdes.String(), jsonSerde));

        // retrieve values from json nodes
        KStream<String, Integer> integerValues = egvs
                .mapValues(egv -> egv.get("value").asInt());
        integerValues.to("integer-values", Produced.with(Serdes.String(), Serdes.Integer()));

        // categorize egv into in-range/out-of-range based on range: 75 <= x <= 180
        KStream<String, String> areValuesInRange = integerValues
                .mapValues(integerValue -> (integerValue >= 75 && integerValue <= 180) ? "true" : "false");
        areValuesInRange.to("are-values-in-range", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
