package com.it.flink

import java.net.URL

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

/**
 * flink 批处理计算
 */
object BatchWordcount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val url: URL = getClass.getResource("/wc.txt")
    val data: DataSet[String] = env.readTextFile(url.getPath) // DataSet ==> spark RDD

    data.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
    env.execute()
  }
}
