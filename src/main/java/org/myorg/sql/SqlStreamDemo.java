package org.myorg.sql;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.myorg.model.MySource;



public class SqlStreamDemo {

    public static void main(String[] args) throws Exception {
        // 获取流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建 table api 和 SQL 执行程序的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 获取自定义的数据流
        DataStreamSource<String> stream = env.addSource(new MySource());

        // 将DataStream 转为Table
        Table table = tEnv.fromDataStream(stream);

        // register the Table object as a view and query it
        tEnv.createTemporaryView("InputTable", table);
        Table result = tEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");


        // 讲给定的 Table 转换为指定类型的 DateStream
        // interpret the insert-only Table as a DataStream again
        tEnv.toDataStream(result/*, Row.class*/).print();
        // 执行任务操作。因为Flink 是懒加载的， 所以必须调用 execute() 方法才会执行
        env.execute();
    }
}
