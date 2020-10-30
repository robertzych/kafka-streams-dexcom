package com.github.robertzych.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

public class ProducerDexcom {
    private static final String clientSecret =  System.getenv("client.secret");
    private static final String clientId =  System.getenv("client.id");
    private static final String redirectUri = "http://www.google.com";
    private static final String refreshToken = System.getenv("refresh.token");
    private static final String dexcomApi = "https://api.dexcom.com";
    private static final String bootstrapServers = System.getenv("bootstrap.servers");
    private static final String topic = System.getenv("topic");
    private static final Logger logger = LoggerFactory.getLogger(ProducerDexcom.class);


    public static void main(String[] args) throws IOException {
        String accessToken = refreshAccessToken();
        List<Egv> egvs = getEstimatedGlucoseValues(accessToken);
        sendEgvs(egvs);
    }

    private static void sendEgvs(List<Egv> egvs) {
        final KafkaProducer<String, String> producer = createKafkaProducer();

        for(Egv egv : egvs)
        {
            // TODO: Use AVRO or Protobuf instead of JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String json = null;
            try {
                json = objectMapper.writeValueAsString(egv);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                logger.error("Failed to map egv to json!", e);
            }
            if (json != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, json);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to send to Kafka", exception);
                    }
                });
                logger.info(json);
            }
        }

        producer.flush();
        producer.close();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // send a producer request id to allow the broker to dedup
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // request the broker to ack only when all replicas have acked
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // These settings are implied with idempotence, just setting them explicitly for clarity
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // use the snappy compression algo to compress the message batches before sending to Kafka
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // configure producer to wait 20 ms before sending a batch
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        // configure a larger batch size to increase throughput
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB

        return new KafkaProducer<>(properties);
    }

    private static List<Egv> getEstimatedGlucoseValues(String accessToken) throws IOException {
        String startDate = "2020-10-11T12:00:00";
        String endDate = "2020-10-11T12:10:00";
        URL url = new URL(dexcomApi + String.format("/v2/users/self/egvs?startDate=%s&endDate=%s", startDate, endDate));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("authorization", String.format("Bearer %s", accessToken));
        con.setDoOutput(true);

        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), "utf-8"))){
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null){
                response.append(responseLine.trim());
            }
            ObjectMapper objectMapper = new ObjectMapper();
            DexcomEgvsResponse dexcomEgvsResponse = objectMapper.readValue(response.toString(), DexcomEgvsResponse.class);
            return dexcomEgvsResponse.egvs;
        }
    }

    private static String refreshAccessToken() throws IOException {
        URL url = new URL(dexcomApi + "/v2/oauth2/token");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        con.setRequestProperty("Cache-Control", "no-cache");
        con.setDoOutput(true);

        // create the request body
        String payload = String.format("client_secret=%s&client_id=%s&refresh_token=%s&grant_type=refresh_token&redirect_uri=%s",
                clientSecret, clientId, refreshToken, redirectUri);
        try(OutputStream os = con.getOutputStream()){
            byte[] input = payload.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        // read the response from the input stream
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), "utf-8"))){
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null){
                response.append(responseLine.trim());
            }
            ObjectMapper objectMapper = new ObjectMapper();
            DexcomTokenResponse dtr = objectMapper.readValue(response.toString(), DexcomTokenResponse.class);
            return dtr.access_token;
        }
    }
}

