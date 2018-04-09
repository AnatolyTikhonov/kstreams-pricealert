package com.mentory.pricealert.topolgy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mentory.pricealert.model.Flight;
import com.mentory.pricealert.model.PriceAlert;
import com.mentory.pricealert.model.PriceRequest;
import com.mentory.pricealert.util.JsonPOJODeserializer;
import com.mentory.pricealert.util.JsonPOJOSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by nimrodti on 8/13/17.
 */
@Component
public class KsTopologyBuilder {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private static final String SDD_FORMAT = "%s::%s::%s";

    private KStreamBuilder builder;
    private ObjectMapper om;
    private Properties streamsConfiguration;
    private KafkaStreams streams;
    private Serde<PriceRequest> priceRequestSerde;
    private Serde<Flight> flightSerde;
    private Serde<HashSet<String>> aggregateUsersSerde;
    private Serde<HashSet<Flight>> aggregateFlightsSerde;
    private Serde<PriceAlert> priceAlertSerde;

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    public void buildAndDeploy() {
        builder = new KStreamBuilder();
        om = new ObjectMapper();

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "price-alert");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        prepareSerializers();
        wireAnomalies();
    }

    public void wireAnomalies() {
        if (streams != null) {
            closeKafkaStreams();
        }

        KStream<String, PriceRequest> inputRequestsStream = builder.stream(Serdes.String(), priceRequestSerde, "requests");

        KStream<String, Flight> inputFlightsStream = builder.stream(Serdes.String(), flightSerde, "flights");

        KTable<String, HashSet<String>> aggregatedRequestsTable = inputRequestsStream
                .groupBy((key, value) -> toSdd(SDD_FORMAT, value.getSrc().name(), value.getDest().name(), value.getDate()),
                        Serdes.String(),
                        priceRequestSerde)
                .aggregate(
                        () -> new HashSet<>(),
                        (aggKey, newValue, aggValue) -> {
                            aggValue.add(newValue.getUser());
                            logger.info("Aggregating requests: " + aggKey.toString() + "  ###  " + aggValue.toString());
                            return aggValue;
                            },
                        aggregateUsersSerde,
                        "aggregated-stream-store"
                );

        KTable<Windowed<String>, HashSet<Flight>> windowedAggregatedFlightsTable = inputFlightsStream
                .groupBy((key, value) -> toSdd(SDD_FORMAT, value.getSrc().name(), value.getDest().name(), value.getDate()),
                        Serdes.String(),
                        flightSerde)
                .aggregate(
                        () -> new HashSet<>(),
                        (aggKey, newValue, aggValue) -> {
                            aggValue.add(newValue);
                            logger.info("Aggregating flights: " + aggKey.toString() + "  ###  " + aggValue.toString());
                            return aggValue;
                        },
                        TimeWindows.of(TimeUnit.SECONDS.toMillis(60)),
                        aggregateFlightsSerde,
                        "time-windowed-aggregated-stream-store"
                );

        KStream<String, PriceAlert> priceAlertStream = windowedAggregatedFlightsTable
                .toStream()
                .peek((key, value) -> logger.info("Alerts before join: " + key.key() + "  ###  " + value.toString()))
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(aggregatedRequestsTable,
                        PriceAlert::new,
                        Serdes.String(),
                        aggregateFlightsSerde
                        );

        priceAlertStream.to(Serdes.String(), priceAlertSerde, "alerts");

        streams = new KafkaStreams(builder, streamsConfiguration);
        streams.cleanUp();
        streams.start();
    }

    @PreDestroy
    public void closeKafkaStreams() {
        streams.close();
        logger.info("Old application closed");
    }

    private String toSdd(String format, String src, String dest, Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return String.format(format, src, dest, dateFormat.format(date));
    }


    private void prepareSerializers() {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<PriceRequest> priceRequestSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PriceRequest.class);
        priceRequestSerializer.configure(serdeProps, false);

        final Deserializer<PriceRequest> priceRequestDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PriceRequest.class);
        priceRequestDeserializer.configure(serdeProps, false);

        priceRequestSerde = Serdes.serdeFrom(priceRequestSerializer, priceRequestDeserializer);

        final Serializer<Flight> flightSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Flight.class);
        flightSerializer.configure(serdeProps, false);

        final Deserializer<Flight> flightDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Flight.class);
        flightDeserializer.configure(serdeProps, false);

        flightSerde = Serdes.serdeFrom(flightSerializer, flightDeserializer);

        final Serializer<HashSet<String>> aggregateUsersSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", HashSet.class);
        aggregateUsersSerializer.configure(serdeProps, false);

        final Deserializer<HashSet<String>> aggregateUsersDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", HashSet.class);
        aggregateUsersDeserializer.configure(serdeProps, false);

        aggregateUsersSerde = Serdes.serdeFrom(aggregateUsersSerializer, aggregateUsersDeserializer);

        final Serializer<HashSet<Flight>> aggregateFlightsSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", HashSet.class);
        aggregateFlightsSerializer.configure(serdeProps, false);

        final Deserializer<HashSet<Flight>> aggregateFlightsDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", HashSet.class);
        aggregateFlightsDeserializer.configure(serdeProps, false);

        aggregateFlightsSerde = Serdes.serdeFrom(aggregateFlightsSerializer, aggregateFlightsDeserializer);

        final Serializer<PriceAlert> priceAlertSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PriceAlert.class);
        priceAlertSerializer.configure(serdeProps, false);

        final Deserializer<PriceAlert> priceAlertDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", PriceAlert.class);
        priceAlertDeserializer.configure(serdeProps, false);

        priceAlertSerde = Serdes.serdeFrom(priceAlertSerializer, priceAlertDeserializer);
    }
}
