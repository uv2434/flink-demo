package com.it.flink.window

import com.it.flink.source.StationLog
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 统计每隔5s统计每隔基站的日志数量
 */
object ReduceFunctionByWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node1", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    stream.map(log => (log.sid, 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .print()
    env.execute()
  }
}
