package com.it.flink.tableAndSql

import com.it.flink.source.StationLog
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, UnresolvedFieldExpression}
import org.apache.flink.types.Row


/**
 * 从文件中创建表（静态表），批计算
 */
object TestCreateTableByStream {
  def main(args: Array[String]): Unit = {
    /**
     * 使用Flink原生代码创建TableEnvironment
     * 先初始化流计算的上下文
     */
    //使用Flink原生的代码创建TableEnvironment
    //先初始化流计算的上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    //读取数据
    //读取数据源
    val stream: DataStream[StationLog] = env.socketTextStream("node1", 8888)
      .map(line => {
        var arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })
    //注册一张表,方法没有返回值
    tableEnv.registerDataStream("t_table2", stream)

    tableEnv.sqlQuery("select * from t_table2")
      .printSchema()
    //    tableEnv.scan("t_table2")
    // 字段改名
    //    val table: Table = tableEnv.fromDataStream(stream, 'sid as 'id, 'callTime as 'call_time)
    //    table.printSchema()

    val table: Table = tableEnv.fromDataStream(stream)
    //过滤查询
    tableEnv.toAppendStream[Row](
      table.filter("callType==='success'")
      //filter
    ) //where
      .print()

    /**
     *
     */
    env.execute()

  }
}
