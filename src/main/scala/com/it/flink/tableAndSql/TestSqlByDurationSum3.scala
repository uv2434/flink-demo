package com.it.flink.tableAndSql


import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

object TestSqlByDurationSum3 {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    val path: String = getClass.getResource("/station.log").getPath
    // 对已注册的表进行 SQL 查询
    // 注册名为 “t_station_log” 的表

    val DDL: String =
      """
        |create table t_station_log (
        |sid STRING,
        |call_out  STRING,
        |call_in  STRING,
        |call_type  STRING,
        |call_time  BIGINT,
        |duration  BIGINT
        |) with (
        |    'format.type' = 'csv',
        |    'connector.type' = 'filesystem',
        |    'connector.path' = 'T:\IdeaProjects\third\flink\src\main\resources\station.log'
        |)
        """.stripMargin
    //    tableEnv.sqlUpdate(DDL)
    tableEnv.executeSql(DDL);

    // 在表上执行 SQL 查询，并把得到的结果作为一个新的表
    var getAllDDL =
      """
        |select * from t_station_log
        |""".stripMargin

    val result = tableEnv.sqlQuery(getAllDDL)

    result.printSchema()
    tableEnv.toRetractStream[Row](result)
      .filter(_._1 == true)
      .print()
    streamEnv.execute()
  }
}
