package org.myorg;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountKafkaInStdOut {

    public static void main(String[] args) throws Exception {

        // 设置Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.137.20:9092");
        // 第 1 个参数是固定值 group.id，第 2 个参数是自定义的组 ID
        properties.setProperty("group.id", "flink-group");
        String inputTopic = "test";
        String outputTopic = "WordCount";

        // Source
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(inputTopic,
                new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);
//        stream.print();
        // Transformation
        // 使用Flink  API对输入流的文本进行操作
        // 按空格切词、计数、分区、设置时间窗口、聚合
        DataStream<Tuple2<String, Integer>> wordCount = stream
                .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] tokens = line.split("\\s");
                    // 输出结果
                    for (String token : tokens) {
                        if (token.length() > 0) {
                            collector.collect(new Tuple2<>(token, 1));
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // Sink
        wordCount.print();
        // execute
        env.execute("kafka streaming word count");
    }
}
