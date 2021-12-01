package com.it.flink.tableAndSql

import com.it.flink.source.StationLog
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object TestSqlByDurationSum2 {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv, settings)

    val stream: DataStream[StationLog] = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        var arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val table: Table = tableEnv.fromDataStream(stream)
    val result: Table = tableEnv.sqlQuery("select sid,sum(duration) as d_c" +
      s" from $table where callType='success' group by sid")
    tableEnv.toRetractStream[Row](result)
      .filter(_._1 == true)
      .print()

    streamEnv.execute("sql")
  }
}
