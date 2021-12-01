package com.it.flink.transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

object TestConnect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1 = env.fromElements(("a", 1), ("b", 2))
    val stream2: DataStream[String] = env.fromElements("c", "d", "e", "f")
    // 实际上得到的数据没有真正合并
    val connStream: ConnectedStreams[(String, Int), String] = stream1.connect(stream2)
    val result: DataStream[(String, Int)] = connStream.map(t => (t._1, t._2), t => (t, 0))

    result.print()

    env.execute()
  }
}
