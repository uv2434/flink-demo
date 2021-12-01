package com.it.stream

import com.it.model.MySource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 实现无界数据流数据源
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new MySource())
      .flatMap {
        _.toLowerCase.split("\\W+") filter {
          _.nonEmpty
        }
      }
      .map((_, 1))
      // 按照tuple中第一个元素进行分组，使用 Hash Partition 实现
      .keyBy(_._1)
      // 时间窗口获取一段时间内的有界数据集
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    stream.print()
    env.execute("Window Stream WordCount")
  }
}
