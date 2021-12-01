package org.myorg.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(12);
//        env.disableOperatorChaining();

//        // 从文件中读取数据
//        String inputPath = "T:\code\flink-demo\src\main\resources\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool工具从程序启动参数中提取配置项

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        System.out.println(host + " " + port);
        // 从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);


        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new MyFlatMapper()).slotSharingGroup("green")
                .keyBy(0)
                .sum(1)
                .setParallelism(2)
                .slotSharingGroup("red");

        resultStream.print().setParallelism(1);

        // 执行任务
        env.execute();

        // 结果前面的编号 为线程编号（本地模式）
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
