package com.it.flink.transformation

import com.it.flink.source.StationLog
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 监控每一个手机号，如果在5秒内呼叫它的通话都是失败的，发出警告信息
 * 在5秒中内只要有一个呼叫不是fail则不用警告
 */
object TestProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取文件数据
    val data = env.socketTextStream("node1", 8888)
      .map(line => {
        var arr = line.split(",")
        StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })
    //处理数据
    data.keyBy(_.callOut)
      .process(new MonitorCallFail())
      .print()
    env.execute()
  }
}

/**
 * 自定义底层类
 */
class MonitorCallFail extends KeyedProcessFunction[String, StationLog, String] {
  // 使用状态对象记录时间
  lazy val timeState: ValueState[Long] = getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

  override def processElement(value: StationLog, ctx: KeyedProcessFunction[String, StationLog, String]#Context, out: Collector[String]): Unit = {
    //从状态中取得时间
    var time = timeState.value()
    if (value.callType.equals("fail") && time == 0) { //表示第一次发现呼叫当前手机号是失败的
      //获取当前时间，并注册定时器
      var nowTime = ctx.timerService().currentProcessingTime()
      var onTime = nowTime + 5000L //5秒后触发
      ctx.timerService().registerProcessingTimeTimer(onTime)
      timeState.update(onTime)
    }
    if (!value.callType.equals("fail") && time != 0) { //表示有呼叫成功了，可以取消触发器
      ctx.timerService().deleteProcessingTimeTimer(time)
      timeState.clear()
    }
  }

  //时间到了，执行触发器,发出告警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, StationLog, String]#OnTimerContext, out: Collector[String]): Unit = {
    var warnStr = "触发时间:" + timestamp + " 手机号：" + ctx.getCurrentKey
    out.collect(warnStr)
    timeState.clear()
  }
}