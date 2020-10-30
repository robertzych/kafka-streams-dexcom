package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class CategorizeWithSimpleRuleTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, JsonNode> egvsTopic;
    private TestOutputTopic<String, Integer> integersTopic;
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
        CategorizeWithSimpleRule categorizeWithSimpleRule = new CategorizeWithSimpleRule();
        Topology topology = categorizeWithSimpleRule.createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        // setup test topics
        egvsTopic = testDriver.createInputTopic(
                "egvs_topic", stringSerde.serializer(), jsonSerde.serializer());
        integersTopic = testDriver.createOutputTopic(
                "integer-values", stringSerde.deserializer(), integerSerde.deserializer());
        inRangeTopic = testDriver.createOutputTopic(
                "are-values-in-range", stringSerde.deserializer(), stringSerde.deserializer());

        // inserts egvs into the egvs_topic
        ObjectNode lowEgv = JsonNodeFactory.instance.objectNode();
        lowEgv.put("value", 65);
        egvsTopic.pipeInput("robert", lowEgv);

        ObjectNode inRangeEgv = JsonNodeFactory.instance.objectNode();
        inRangeEgv.put("value", 100);
        egvsTopic.pipeInput("robert", inRangeEgv);

        ObjectNode highEgv = JsonNodeFactory.instance.objectNode();
        highEgv.put("value", 265);
        egvsTopic.pipeInput("robert", highEgv);
    }

    @After
    public void tearDown(){
        testDriver.close();
    }

    @Test
    public void ShouldOutputIntValues(){
        int actual = integersTopic.readValue();
        assertEquals(65, actual);
    }

    @Test
    public void ShouldCategorizeInRange(){
        Boolean inRange = Boolean.valueOf(inRangeTopic.readValue());
        assertFalse(inRange);
        inRange = Boolean.valueOf(inRangeTopic.readValue());
        assertTrue(inRange);
        inRange = Boolean.valueOf(inRangeTopic.readValue());
        assertFalse(inRange);
    }
}
