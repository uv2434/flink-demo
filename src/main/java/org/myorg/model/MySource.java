package org.myorg.model;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

public class MySource implements SourceFunction<String> {

    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning) {
            ArrayList<String> stringList = new ArrayList<>();

            stringList.add("world");
            stringList.add("Flink");
            stringList.add("Stream");
            stringList.add("Batch");
            stringList.add("Table");
            stringList.add("SQL");
            stringList.add("Hello");

            int size = stringList.size();

            int i = new Random().nextInt(size);
            sourceContext.collect(stringList.get(i));

            // 每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
