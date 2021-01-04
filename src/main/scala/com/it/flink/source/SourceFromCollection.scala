package com.it.flink.source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 *
 * @param sid      基站id
 * @param callOut  主叫号码
 * @param callIn   被叫号码
 * @param callType 呼叫类型
 * @param callTime 呼叫时间(毫秒)
 * @param duration 通话时长(s)
 */
case class StationLog(sid: String, var callOut: String, var callIn: String, callType: String, callTime: Long, duration: Long)

object SourceFromCollection {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[StationLog] = streamEnv.fromCollection(Array(
      StationLog("001", "1866", "189", "busy", System.currentTimeMillis(), 0),
      StationLog("002", "1867", "189", "success", System.currentTimeMillis(), 50),
      StationLog("003", "1868", "188", "busy", System.currentTimeMillis(), 0),
      StationLog("004", "1869", "188", "success", System.currentTimeMillis(), 20)
    ))

    stream.print()
    streamEnv.execute()
  }
}
