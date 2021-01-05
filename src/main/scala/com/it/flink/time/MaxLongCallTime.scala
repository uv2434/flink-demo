package com.it.flink.time

import com.it.flink.source.StationLog
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 每隔5秒统计最近10秒内，每个基站中通话时间最长的一次通话发生的时间、主叫号码、被叫号码、通话时长
 * 以及当前发生的时间范围（10秒）
 */
object MaxLongCallTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("node1", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })
      // 引入watermark(默认数据是有序)
      // 参数中指定eventTime集体的值是什么
      .assignAscendingTimestamps(_.callTime)

    stream.filter(_.callType.equalsIgnoreCase("success"))
      .keyBy(_.sid)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce(new MyReduceFunction, new GetMaxTimeWindowFunction)
      .print()
    env.execute()
  }
}

class MyReduceFunction extends ReduceFunction[StationLog] {
  override def reduce(t: StationLog, t1: StationLog): StationLog = {
    if (t.duration > t1.duration) t else t1
  }
}

class GetMaxTimeWindowFunction extends WindowFunction[StationLog, String, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[StationLog], out: Collector[String]): Unit = {

    var value = input.iterator.next
    var sb = new StringBuilder
    sb.append("窗口的范围是：")
      .append(window.getStart)
      .append("---------")
      .append(window.getEnd)
      .append("\n")
      .append("呼叫时间：").append(value.callTime)
      .append("主叫号码：").append(value.callOut)
      .append("被叫号码：").append(value.callIn)
      .append("通话时长：").append(value.duration)
    out.collect(sb.toString())
  }
}
