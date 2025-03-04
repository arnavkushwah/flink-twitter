package com.arnav;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class FlinkKafkaConsumer {
    public static void main(String[] args) throws Exception {
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("twitter-stream")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Read tweets from Kafka
        DataStream<String> stream = env.fromSource(source, 
                                                  org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
                                                  "Kafka Source");

        // Print received tweets
        stream.print();

        // Execute Flink job
        env.execute("Flink Kafka Consumer");
    }
}