package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EgvTransformer implements ValueTransformer<JsonNode, JsonNode> {

    private KeyValueStore<Integer, JsonNode> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public EgvTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.stateStore = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    public JsonNode transform(JsonNode egv) {
        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");
        KeyValueIterator<Integer, JsonNode> iterator = stateStore.all();
        while (iterator.hasNext()){
            // TODO: enrich the egv with lowerBound and upperBound from the first matching range
            KeyValue<Integer, JsonNode> range = iterator.next();
            String start_time = range.value.get("start_time").asText();
            try {
                Date start = dateFormat.parse(start_time);
            } catch (ParseException e) {
                e.printStackTrace();
                continue;
            }
        }
        iterator.close();
        return egv;
    }

    @Override
    public void close() {

    }
}
