package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class CategorizeWithKTableLookup {

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

        CategorizeWithKTableLookup categorizeWithSimpleRule = new CategorizeWithKTableLookup();

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

        // input topics
        KStream<String, JsonNode> egvs = builder.stream("egvs_topic",
                Consumed.with(Serdes.String(), jsonSerde));

        String rangesStateStoreName = "rangesStore";
//        KTable<Integer, JsonNode> ranges = builder.table("ranges_topic",
//                Consumed.with(Serdes.Integer(), jsonSerde),
//                Materialized.as(rangesStateStoreName));
        builder.stream("ranges_topic", Consumed.with(Serdes.Integer(), jsonSerde))
                .map((key, value) -> {
                    String newKey = String.format("%d-%s", key, Instant.now().toString());
                    return KeyValue.pair(newKey, value);
                }).to("ranges-rekeyed", Produced.with(Serdes.String(), jsonSerde));
        builder.table("ranges-rekeyed",
                Consumed.with(Serdes.String(), jsonSerde),
                Materialized.as(rangesStateStoreName));

        // enrich the egvs with lower/upper bounds from a matching range
        KStream<String, JsonNode> enrichedEgvs =
                egvs.transformValues(() -> new EgvTransformer(rangesStateStoreName), rangesStateStoreName);

        // categorize the enriched egvs inRange=True based on the lower/upper bounds
        KStream<String, String> areValuesInRange = enrichedEgvs
                .mapValues(enrichedEgv -> {
                    int integerValue = enrichedEgv.get("value").asInt();
                    int lowerBound = enrichedEgv.get("lower_bound").asInt();
                    int upperBound = enrichedEgv.get("upper_bound").asInt();
                    return (integerValue >= lowerBound && integerValue <= upperBound) ? "true" : "false";
                });
        areValuesInRange.to("are-values-in-range", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
