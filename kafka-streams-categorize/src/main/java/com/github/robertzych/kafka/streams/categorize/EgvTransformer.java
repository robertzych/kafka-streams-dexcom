package com.github.robertzych.kafka.streams.categorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class EgvTransformer implements ValueTransformer<JsonNode, JsonNode> {

    private TimestampedWindowStore<Integer, JsonNode> stateStore;
    private final String storeName;
    private ProcessorContext context;

    public EgvTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.stateStore = (TimestampedWindowStore) this.context.getStateStore(storeName);
    }

    @Override
    public JsonNode transform(JsonNode egv) {
        String systemTime = egv.get("systemTime").asText();
        Instant systemInstant = Instant.parse(systemTime + "Z");
        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");
        String egvTime = systemTime.split(String.valueOf('T'))[1];
        Date egvDate;
        try {
            egvDate = dateFormat.parse(egvTime);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
        Instant egvInstant = egvDate.toInstant();

        KeyValueIterator<Windowed<Integer>, ValueAndTimestamp<JsonNode>> iterator = stateStore.all();
        ObjectNode enrichedEgv = null;
        while (iterator.hasNext()){
            KeyValue<Windowed<Integer>, ValueAndTimestamp<JsonNode>> record = iterator.next();
            Date recordUpdated = new Date(record.value.timestamp());
            Instant recordInstant = recordUpdated.toInstant();
            // check if the range record was updated after the egv was created
            if (recordInstant.compareTo(systemInstant) > 0) {
                continue;
            }
            JsonNode range = record.value.value();
            String start_time = range.get("start_time").asText();
            Date startDate;
            try {
                startDate = dateFormat.parse(start_time);
            } catch (ParseException e) {
                e.printStackTrace();
                continue;
            }
            Instant startInstant = startDate.toInstant();
            String end_time = range.get("end_time").asText();
            Date endDate;
            try {
                endDate = dateFormat.parse(end_time);
            } catch (ParseException e) {
                e.printStackTrace();
                continue;
            }
            Instant endInstant = endDate.toInstant();
            if (egvInstant.compareTo(startInstant) >= 0 && egvInstant.compareTo(endInstant) <= 0) {
                enrichedEgv = JsonNodeFactory.instance.objectNode();
                int value = egv.get("value").asInt();
                enrichedEgv.put("value", value);
                enrichedEgv.put("lower_bound", range.get("lower_bound").asInt());
                enrichedEgv.put("upper_bound", range.get("upper_bound").asInt());
                break;
            }
        }
        iterator.close();
        return enrichedEgv;
    }

    @Override
    public void close() {

    }
}
