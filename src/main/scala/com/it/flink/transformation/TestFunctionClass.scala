package com.it.flink.transformation

import java.text.SimpleDateFormat

import com.it.flink.source.StationLog
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestFunctionClass {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val path: String = getClass.getResource("/station.log").getPath()
    val stream: DataStream[StationLog] = env.readTextFile(path)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    stream.filter(_.callType.equals("success"))
      .map(new MyMapFunction(format))
      .print()
    env.execute()
  }
}

/**
 * 继承MapFunction实现自定义函数类
 *
 * @param format
 */
class MyMapFunction(format: SimpleDateFormat) extends MapFunction[StationLog, String] {
  override def map(t: StationLog): String = {
    var startTime = t.callTime
    var endTime = t.callTime + t.duration * 1000
    "主叫号码:" + t.callIn + ",被叫号码:" + t.callOut + ",起始时间:" + format.format(startTime) +
      ",结束时间:" + format.format(endTime)
  }
}