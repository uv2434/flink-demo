package com.it.flink.time

import com.it.flink.source.StationLog
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 每隔5秒统计最近10秒内，每个基站中通话时间最长的一次通话发生的时间、主叫号码、被叫号码、通话时长
 * 以及当前发生的时间范围（10秒）
 */
object MaxLongCallTime2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("node1", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })
      //引入Watermark(数据乱序的)，并且通过观察延迟的时间是3秒,采用周期性的Watermark引入
      //代码有两种写法，
      //第一种：直接采用AssignerWithPeriodicWatermarks接口的实现类（Flink提供的）
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(3)) {
        override def extractTimestamp(element: StationLog) = { //设置我们的EventTime
          element.callTime //就是EventTime
        }
      })
    //第二种：自己定义一个AssignerWithPeriodicWatermarks接口的实现类
    //      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StationLog]{
    //        var maxEventTime :Long=_
    //        override def getCurrentWatermark = {//周期性的生成Watermark
    //          new Watermark(maxEventTime-3000L)
    //        }
    //        //设定EventTime是哪个属性
    //        override def extractTimestamp(element: StationLog, previousElementTimestamp: Long) = {
    //          maxEventTime =maxEventTime.max(element.callTime)
    //          element.callTime
    //        }
    //      })


    stream.filter(_.callType.equalsIgnoreCase("success"))
      .keyBy(_.sid)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce(new MyReduceFunction, new GetMaxTimeWindowFunction)
      .print()
    env.execute()
  }
}
