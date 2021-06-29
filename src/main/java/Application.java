
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Application {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamApp3");
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put("auto.offset.reset", "latest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder.stream("test");

        KTable<String, String> joinTable =  streamsBuilder.table("testone");

        //Created branches for different processing of same event
        KStream<String, String>[] branches = stream.branch((key, value) -> value != null);

        //split the value by space and send events for each value occupied after splitting
        branches[0].flatMapValues(value ->
                Arrays.asList(value.toLowerCase().split(" "))
        ).selectKey((key, value) -> value + "key").to("events", Produced.with(Serdes.String(), Serdes.String()));

        //Aggregation
        branches[0].flatMapValues(value ->
                Arrays.asList(value.toLowerCase().split(" "))
        ).selectKey((key, value) -> value + "key").groupByKey().count().
                mapValues(
                        (key,value) -> value.toString()).toStream().to("events", Produced.with(Serdes.String(), Serdes.String()));

        //filter the value, join the key with other KTable and send the event
        branches[0].filter(
                    (key, value) ->
                            value.contains("second example")).join(joinTable, (ValueJoiner<String, String, Object>) (s, s2) -> s + s2).mapValues((event, value) ->
                    value + " newvalue"
            ).to("eventsone", Produced.with(Serdes.String(), Serdes.String()));

        //filter the value, left join the key with other KTable and send the event
        branches[0].filter(
                (key, value) ->
                        value.contains("second example")).leftJoin(joinTable, (ValueJoiner<String, String, Object>) (s, s2) -> s + s2).mapValues((event, value) ->
                value + " newvalue"
        ).to("eventsone", Produced.with(Serdes.String(), Serdes.String()));



        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
        streams.setUncaughtExceptionHandler((e, ex) -> {
            System.out.println(e);
            System.out.println(ex);
            System.exit(1);
        });

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread (() ->
                streams.close()
        ));
    }
}
