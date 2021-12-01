package com.it.flink.window

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("node1", 9999)

    val counts = text.flatMap {
      _.toLowerCase.split(" ")
        .filter (_.nonEmpty)
    }.map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    counts.print()
    env.execute("Window Stream WordCount")
  }
}
