package com.infinitelambda;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;


public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("word-count-input")
                .setGroupId("flink-wordcount")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        DataStream<Tuple2<String, Integer>> counts = text
            .map(new Tokenizer())
            .keyBy(item -> item.f0)
            //.timeWindow(Time.seconds(30))
            .sum(1);

//        DataStreamSink<Object> counts = text
//            .timeWindowAll(Time.seconds(5)).process(
//                new ProcessAllWindowFunction<>() {
//                    @Override
//                    public void process(Context context, Iterable<String> elements,
//                        Collector<Object> out) throws Exception {
//                        elements.forEach(e -> {
//                                String[] params = e.toLowerCase().split(" ");
//                                String timestamp = params[0];
//                                String count = params[1];
//                                System.out.println(timestamp + " " + count);
//                                Tuple2<String, Integer> tuple2 = new Tuple2<>(timestamp,
//                                    Integer.parseInt(count));
//                                System.out.println(tuple2);
//                                out.collect(tuple2);
//                            }
//                        );
//                    }
//                }).print();

        KafkaSink<Tuple2<String, Integer>> sink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("word-count-output")
                        .setValueSerializationSchema(new SerSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        counts.sinkTo(sink);

        env.execute("Kafka WordCount");
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            String[] params = s.toLowerCase().split(" ");
            String timestamp = params[0];
            String count = params[1];
            System.out.println(timestamp + " " + count);
            Tuple2<String, Integer> tuple2 = new Tuple2<>(timestamp, Integer.parseInt(count));
            System.out.println(tuple2);
            return tuple2;
        }
    }

    private static final class SerSchema implements SerializationSchema<Tuple2<String, Integer>> {
        @Override
        public byte[] serialize(Tuple2<String, Integer> stringIntegerTuple2) {
            return (stringIntegerTuple2.f0 + ":" + stringIntegerTuple2.f1.toString()).getBytes(StandardCharsets.UTF_8);
        }
    }
}
