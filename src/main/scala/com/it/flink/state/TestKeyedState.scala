package com.it.flink.state

import com.it.flink.source.StationLog
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TestKeyedState {
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
      .flatMap(new CallIntervalFunction())
    result.print("时间间隔为：")
    env.execute()
  }
}

/**
 * 统计每个手机的呼叫间隔时间，并输出
 * 富函数
 */
class CallIntervalFunction() extends RichFlatMapFunction[StationLog, (String, Long)] {

  //定义一个保存前一条呼叫的数据的状态对象,preData.value()即为前一条状态的值
  private var preData: ValueState[StationLog] = _

  override def open(parameters: Configuration): Unit = {
    preData = getRuntimeContext.getState(
      new ValueStateDescriptor[StationLog]("pre", classOf[StationLog]))
  }

  override def flatMap(in: StationLog, collector: Collector[(String, Long)]): Unit = {
    var pre: StationLog = preData.value()
    if (pre == 0 || pre == null) {
      preData.update(in)
    } else {
      val interval = pre.callTime
      collector.collect(in.callIn, in.callTime - interval)
    }
  }
}