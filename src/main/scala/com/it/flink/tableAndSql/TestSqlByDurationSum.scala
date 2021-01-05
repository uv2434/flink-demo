package com.it.flink.tableAndSql

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object TestSqlByDurationSum {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    val tableSource = new CsvTableSource("T:\\IdeaProjects\\third\\flink\\src\\main\\resources\\station.log",
      Array[String]("sid", "call_out", "call_in", "call_type", "call_time", "duration"),
      Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG))

    /**
     * 使用纯粹的SQL
     */

    tableEnv.registerTableSource("t_station_log", tableSource)
    val result: Table = tableEnv.sqlQuery("select sid,sum(duration) as d_c" +
      " from t_station_log where call_type='success' group by sid")
    tableEnv.toRetractStream[Row](result)
      .filter(_._1 == true)
      .print()

    streamEnv.execute("sql")
  }
}
