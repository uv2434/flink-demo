package com.it.flink.source

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/**
 * 自定义source，
 * 每隔2秒钟，生成10条随机的基站通话日志数据
 */
class MyCustomerSource extends SourceFunction[StationLog] {
  var flag = true;

  /**
   * 主要的方法，启动一个source，并从source中返回数据
   * 如果run方法停止，则数据流终止
   *
   * @param sourceContext
   */
  override def run(sourceContext: SourceFunction.SourceContext[StationLog]): Unit = {
    val r = new Random()
    val types: Array[String] = Array[String]("failed", "busy", "success", "barring")
    while (flag) {
      1.to(10).map(i => {
        var callOut = "1860000%04d".format(r.nextInt(10000)) //主叫号码
        var callIn = "1890000%04d".format(r.nextInt(10000)) //主叫号码
        StationLog("station_" + r.nextInt(10), callOut, callIn, types(r.nextInt(4)), System.currentTimeMillis(), r.nextInt(10))
      }).foreach(sourceContext.collect(_)) // 发送到数据流
      Thread.sleep(3000)
    }
  }

  /**
   * 终止数据流
   */
  override def cancel(): Unit = {
    flag = false
  }
}

object SourceFromCustomer {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    val stream: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource())
    stream.print()
    streamEnv.execute("SourceFromCustomer")
  }
}
