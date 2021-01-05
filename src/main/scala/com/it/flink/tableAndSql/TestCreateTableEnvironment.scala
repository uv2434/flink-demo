package com.it.flink.tableAndSql

// **********************

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


object TestCreateTableEnvironment {
  def main(args: Array[String]): Unit = {
    /**
     * 使用Flink原生代码创建TableEnvironment
     * 先初始化流计算的上下文
     */
    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
    val bsTableEnv = TableEnvironment.create(bsSettings)


//    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    //    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)
    // or val fsTableEnv = TableEnvironment.create(fsSettings)
  }
}
