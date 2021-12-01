package com.it.flink.source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SourceFromHDFS {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val lines: DataStream[String] = streamEnv.readTextFile("hdfs://node2/wc.txt")
    val result: DataStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    result.print()
    streamEnv.execute("wordcount")
  }
}
