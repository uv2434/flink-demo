package com.it.flink.transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestUnion {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[(String, Int)] = env.fromElements(("a", 1), ("b", 2))
    val stream2: DataStream[(String, Int)] = env.fromElements(("b", 5), ("d", 6))
    val stream3: DataStream[(String, Int)] = env.fromElements(("e", 7), ("d", 8))

    val result: DataStream[(String, Int)] = stream1.union(stream2, stream3)

    result.print()
    env.execute()
  }
}
