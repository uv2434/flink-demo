/*
package com.it.table

import com.it.model.MySource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{$, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object WordCountTableForStream {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    import org.apache.flink.table.api._
    import org.apache.flink.table.api.bridge.scala._

    // environment configuration
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = TableEnvironment.create(settings)

    // register Orders table in table environment
    // ...

    // table is the result of a simple projection query
    val projTable: Table = tEnv.from("X")

    // register the Table projTable as table "projectedTable"
    tEnv.createTemporaryView("Orders", projTable)

    // specify table program
    val orders = tEnv.from("Orders") // schema (a, b, c, rowtime)

    val result = orders
      .groupBy($"a")
      .select($"a", $"b".count as "cnt")
      .toDataSet[Row] // conversion to DataSet
      .print()

  }
}
*/

