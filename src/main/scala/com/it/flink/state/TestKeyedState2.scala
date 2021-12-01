package com.it.flink.state

import com.it.flink.source.StationLog
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestKeyedState2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val path: String = getClass.getResource("/station.log").getPath()
    val stream: DataStream[StationLog] = env.readTextFile(path)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    val result: DataStream[(String, Long)] = stream
      .keyBy(_.callIn)
      .mapWithState[(String, Long), StationLog] {
        case (in: StationLog, None) => ((in.callIn, 0), Some(in))
        case (in: StationLog, pre: Some[StationLog]) => {
          var interval = in.callTime - pre.value.callTime
          ((in.callIn, interval), Some(in))
        }
      }
    result.print("时间间隔为：")
    env.execute()
  }
}