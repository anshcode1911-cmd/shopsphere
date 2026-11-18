package com.shopsphere.analytics;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Real-time analytics job (runs on Flink on a Dataproc cluster).
 *
 * <pre>
 *  Kafka `shop-events`  --(event-time, 10 s out-of-orderness)-->
 *      keyBy(eventType) -> 1-minute tumbling window ->
 *      {eventCount, uniqueUsers (distinct user_id), revenue} ->
 *  Kafka `analytics-results`
 * </pre>
 *
 * Arguments: --bootstrap host:9092 --username sa@... --password base64key
 *            [--input shop-events] [--output analytics-results] [--window-seconds 60]
 *            [--checkpoint-dir gs://bucket/flink-checkpoints] [--security-protocol SASL_SSL]
 */
public class ShopEventsWindowJob {

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String bootstrap = p.getRequired("bootstrap");
        String input = p.get("input", "shop-events");
        String output = p.get("output", "analytics-results");
        long windowSeconds = p.getLong("window-seconds", 60L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
        if (p.has("checkpoint-dir")) {
            env.getCheckpointConfig().setCheckpointStorage(p.get("checkpoint-dir"));
        }

        Properties kafkaProps = kafkaSecurity(p);

        KafkaSource<ShopEvent> source = KafkaSource.<ShopEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(input)
                .setGroupId(p.get("group-id", "flink-shop-analytics"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ShopEventDeserializer())
                .setProperties(kafkaProps)
                .build();

        WatermarkStrategy<ShopEvent> watermarks = WatermarkStrategy
                .<ShopEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((e, kafkaTs) -> e.ts > 0 ? e.ts : kafkaTs)
                // partitions without traffic must not stall the event-time clock
                .withIdleness(Duration.ofSeconds(30));

        DataStream<String> results = env
                .fromSource(source, watermarks, "kafka:" + input)
                .uid("kafka-source")
                .keyBy(e -> e.type)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .aggregate(new WindowAggregator(), new WindowResultFunction())
                .uid("window-aggregate")
                .name("1-min tumbling window: events, unique users, revenue");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(kafkaProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(output)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        results.sinkTo(sink).uid("kafka-sink").name("kafka:" + output);

        env.execute("shopsphere-realtime-analytics");
    }

    /** SASL/PLAIN over TLS for Google Cloud Managed Service for Apache Kafka. */
    static Properties kafkaSecurity(ParameterTool p) {
        Properties props = new Properties();
        String protocol = p.get("security-protocol", "SASL_SSL");
        props.setProperty("security.protocol", protocol);
        if (protocol.startsWith("SASL")) {
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + p.getRequired("username") + "\" password=\"" + p.getRequired("password") + "\";");
        }
        return props;
    }
}
