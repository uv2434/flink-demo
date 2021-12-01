package com.it.table

import com.it.model.MyOrder
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.$
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment

object TableBatchDemo {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 创建table api sql的执行环境
    val tEnv = BatchTableEnvironment.create(env)

    val input = env.fromElements(MyOrder(1l, "BMW", 1),
      MyOrder(2l, "Tesla", 8),
      MyOrder(2l, "Tesla", 8),
      MyOrder(3l, "hello-world", 20))

    // 将dataset转化为table
    val table = tEnv.fromDataSet(input)

    val t = table.where($("amount"))

    t.printSchema()
  }
}
