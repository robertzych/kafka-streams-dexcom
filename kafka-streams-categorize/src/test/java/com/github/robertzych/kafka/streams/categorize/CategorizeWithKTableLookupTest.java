package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class CategorizeWithKTableLookupTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, JsonNode> egvsTopic;
    private TestInputTopic<Integer, JsonNode> rangesTopic;
    private TestOutputTopic<Integer, JsonNode> rangesOutputTopic;
    private TestOutputTopic<String, String> inRangeTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
    private Serde<Integer> integerSerde = new Serdes.IntegerSerde();

    @Before
    public void setup(){
        // setup test driver
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        CategorizeWithKTableLookup categorizeWithKTableLookup = new CategorizeWithKTableLookup();
        Topology topology = categorizeWithKTableLookup.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        // setup input topics
        egvsTopic = testDriver.createInputTopic(
                "egvs_topic", stringSerde.serializer(), jsonSerde.serializer());
        rangesTopic = testDriver.createInputTopic(
                "ranges_topic", integerSerde.serializer(), jsonSerde.serializer());

        // setup output topics
        rangesOutputTopic = testDriver.createOutputTopic(
                "ranges-output", integerSerde.deserializer(), jsonSerde.deserializer());
        inRangeTopic = testDriver.createOutputTopic(
                "are-values-in-range", stringSerde.deserializer(), stringSerde.deserializer());


        // inserts egvs into egvs_topic
        ObjectNode lowEarlyMorningEgv = JsonNodeFactory.instance.objectNode();
        lowEarlyMorningEgv.put("value", 75);
        lowEarlyMorningEgv.put("systemTime", "2020-11-02T02:00:00");
        egvsTopic.pipeInput("robert", lowEarlyMorningEgv);

        ObjectNode normalNoonEgv = JsonNodeFactory.instance.objectNode();
        normalNoonEgv.put("value", 100);
        normalNoonEgv.put("systemTime", "2020-11-02T12:00:00");
        egvsTopic.pipeInput("robert", normalNoonEgv);

        ObjectNode highEgv = JsonNodeFactory.instance.objectNode();
        highEgv.put("value", 265);
        highEgv.put("systemTime", "2020-11-02T19:00:00");
        egvsTopic.pipeInput("robert", highEgv);

        ObjectNode highLateEveningEgv = JsonNodeFactory.instance.objectNode();
        highLateEveningEgv.put("value", 160);
        highLateEveningEgv.put("systemTime", "2020-11-02T23:00:00");
        egvsTopic.pipeInput("robert", highEgv);

        // insert ranges into ranges_topic
        ObjectNode whenSleepingRange = JsonNodeFactory.instance.objectNode();
        whenSleepingRange.put("start_time", "22:00:00");
        whenSleepingRange.put("end_time", "05:59:59");
        whenSleepingRange.put("lower_bound", 80);
        whenSleepingRange.put("upper_bound", 150);
        rangesTopic.pipeInput(1, whenSleepingRange);

        ObjectNode whenActiveRange = JsonNodeFactory.instance.objectNode();
        whenActiveRange.put("start_time", "06:00:00");
        whenActiveRange.put("end_time", "21:59:59");
        whenActiveRange.put("lower_bound", 70);
        whenActiveRange.put("upper_bound", 180);
        rangesTopic.pipeInput(2, whenActiveRange);
    }

    @After
    public void tearDown(){
        testDriver.close();
    }

    @Test
    public void RangesTopicHasRanges(){
        JsonNode whenSleepingRange = rangesOutputTopic.readValue();
        int lower_bound = whenSleepingRange.get("lower_bound").asInt();
        assertEquals(80, lower_bound);
    }

    @Test
    public void ShouldCategorizeInRange(){
        boolean lowEarlyMorningEgvInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        assertFalse(lowEarlyMorningEgvInRange);
        boolean normalNoonEgvInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        assertTrue(normalNoonEgvInRange);
        boolean highEgvInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        assertFalse(highEgvInRange);

    }
}
