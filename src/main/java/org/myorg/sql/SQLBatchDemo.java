package org.myorg.sql;

import org.myorg.model.MyOrder;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class SQLBatchDemo {

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        Table table = tableEnv.fromValues(
                new MyOrder(1l, "BMW", 1),
                new MyOrder(2l, "Tesla", 8),
                new MyOrder(2l, "Tesla", 8),
                new MyOrder(3L, "Hello", 20));

        Table amount = table.where($("amount"));

        amount.printSchema();

    }
}
