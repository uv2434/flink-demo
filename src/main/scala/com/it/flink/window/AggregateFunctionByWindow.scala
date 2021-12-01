package com.it.flink.window

import com.it.flink.source.StationLog
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * 统计每隔3s计算最近5s内基站的日志数量
 */
object AggregateFunctionByWindow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node1", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    stream.map(log => (log.sid, 1))
      .keyBy(_._1)
      // 开窗，滑动窗口
      .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(3)))
      //      .timeWindow(Time.seconds(5), Time.seconds(3))
      .aggregate(new MyAggregateFunction, new MyWindowFunction)
    env.execute()
  }
}

/**
 * add方法来一条执行一次
 */
class MyAggregateFunction extends AggregateFunction[(String, Int), Long, Long] {
  // 初始化一个累加器 开始的时候为0
  override def createAccumulator(): Long = 0

  override def add(in: (String, Int), acc: Long): Long = {
    in._2 + acc
  }

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
 * WindowFunction 输入来自 AggregateFunction
 */
class MyWindowFunction extends WindowFunction[Long, (String, Long), String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long)]): Unit = {
    out.collect((key, input.iterator.next())) // next得到第一个值，迭代器中只有一个值
  }


}

