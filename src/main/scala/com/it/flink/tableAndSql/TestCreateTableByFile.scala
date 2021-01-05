package com.it.flink.tableAndSql

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.sources.CsvTableSource

/**
 * 从文件中创建表（静态表），批计算
 */
object TestCreateTableByFile {
  def main(args: Array[String]): Unit = {
    /**
     * 使用Flink原生代码创建TableEnvironment
     * 先初始化流计算的上下文
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //    val tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
    val tableEnv = TableEnvironment.create(settings)

    val tableSource = new CsvTableSource("/stationLog.log",
      Array[String]("f1", "f2", "f3", "f4", "f5", "f6"),
      Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG))

    //注册Table，表名为t_station_log
    tableEnv.registerTableSource("t_station_log", tableSource)

    //转换成Table对象，并打印表结构
    val table: Table = tableEnv.scan("t_station_log")

    table.printSchema()


  }
}
