package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CategorizeOutOfOrderTest {

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

        // setup output topic
        inRangeTopic = testDriver.createOutputTopic(
                "are-values-in-range", stringSerde.deserializer(), stringSerde.deserializer());

        // insert ranges into ranges_topic
        ObjectNode whenSleepingRange = JsonNodeFactory.instance.objectNode();
        whenSleepingRange.put("start_time", "00:00:00");
        whenSleepingRange.put("end_time", "05:59:59");
        whenSleepingRange.put("lower_bound", 80);
        whenSleepingRange.put("upper_bound", 150);
        Instant startTime = Instant.now();
        rangesTopic.pipeInput(1, whenSleepingRange, startTime);

        ObjectNode whenActiveRange = JsonNodeFactory.instance.objectNode();
        whenActiveRange.put("start_time", "06:00:00");
        whenActiveRange.put("end_time", "21:59:59");
        whenActiveRange.put("lower_bound", 70);
        whenActiveRange.put("upper_bound", 180);
        rangesTopic.pipeInput(2, whenActiveRange, startTime.plusSeconds(1));

        ObjectNode eveningRange = JsonNodeFactory.instance.objectNode();
        eveningRange.put("start_time", "22:00:00");
        eveningRange.put("end_time", "23:59:59");
        eveningRange.put("lower_bound", 80);
        eveningRange.put("upper_bound", 150);
        rangesTopic.pipeInput(3, eveningRange, startTime.plusSeconds(2));

        // inserts egvs into egvs_topic
        ObjectNode lowEarlyMorningEgv = JsonNodeFactory.instance.objectNode();
        lowEarlyMorningEgv.put("value", 75);
        lowEarlyMorningEgv.put("systemTime", "2020-11-02T02:00:00");
        egvsTopic.pipeInput("robert", lowEarlyMorningEgv, startTime.plusSeconds(3));

        ObjectNode normalNoonEgv = JsonNodeFactory.instance.objectNode();
        normalNoonEgv.put("value", 100);
        normalNoonEgv.put("systemTime", "2020-11-02T13:00:00");
        egvsTopic.pipeInput("robert", normalNoonEgv, startTime.plusSeconds(4));

        ObjectNode highEgv = JsonNodeFactory.instance.objectNode();
        highEgv.put("value", 265);
        highEgv.put("systemTime", "2020-11-02T19:00:00");
        egvsTopic.pipeInput("robert", highEgv, startTime.plusSeconds(5));

        // setup for out of order
        // increase the upper bound of the eveningRange
        ObjectNode eveningRangeUpdated = JsonNodeFactory.instance.objectNode();
        eveningRangeUpdated.put("start_time", "22:00:00");
        eveningRangeUpdated.put("end_time", "23:59:59");
        eveningRangeUpdated.put("lower_bound", 80);
        eveningRangeUpdated.put("upper_bound", 180); // was: 150
        Instant eveningRangeUpdateTime = startTime.plusSeconds(60);
        rangesTopic.pipeInput(3, eveningRangeUpdated, eveningRangeUpdateTime);

        // highLateEveningEgv arrives after the eveningRange has been updated
        ObjectNode highLateEveningEgv = JsonNodeFactory.instance.objectNode();
        highLateEveningEgv.put("value", 160);
        highLateEveningEgv.put("systemTime", "2020-11-02T23:00:00");
        Instant highLateEveningTime = eveningRangeUpdateTime.plusSeconds(-10);
        egvsTopic.pipeInput("robert", highLateEveningEgv, highLateEveningTime);
    }

    @After
    public void tearDown(){
        testDriver.close();
    }

    @Test
    public void ShouldCategorizeInRange(){
        boolean lowEarlyMorningEgvInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        assertFalse(lowEarlyMorningEgvInRange);
        boolean normalNoonEgvInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        assertTrue(normalNoonEgvInRange);
        boolean highEgvInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        assertFalse(highEgvInRange);
        boolean highLateEveningInRange = Boolean.parseBoolean(inRangeTopic.readValue());
        // this is failing because the eveningRange's upper_bound was changed to 180
        // but should be passing because highLateEvening was produced 10 seconds before the update
        assertFalse(highLateEveningInRange);
    }
}
