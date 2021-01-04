package com.it.flink.cep


import java.util

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * FlinkCEP 中提供了 Pattern API 用于对输入流数据的复杂事件规则定义，并从事件流
 * 中抽取事件结果。包含四个步骤：
 *  输入事件流的创建
 *  Pattern 的定义
 *  Pattern 应用在事件流上检测
 *  选取结果
 */
case class LoginEvent(id: Long, userName: String, eventType: String, eventTime: Long)

object CepEvent2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取登录日志
    val stream = env.fromCollection(List(
      LoginEvent(1, "张三", "fail", 1577080457),
      LoginEvent(2, "张三", "fail", 1577080458),
      LoginEvent(3, "张三", "fail", 1577080460),
      LoginEvent(4, "李四", "fail", 1577080458),
      LoginEvent(5, "李四", "success", 1577080462),
      LoginEvent(6, "张三", "fail", 1577080462)
    ))
      //指定EventTime的时候必须要确保是时间戳（精确到毫秒）
      .assignAscendingTimestamps(_.eventTime * 1000L)
    // 定义pattern
    val pattern = Pattern.begin[LoginEvent]("start")
      .where(_.eventType.equals("fail"))
      .next("fail2").where(_.eventType.equals("fail"))
      .next("fail3").where(_.eventType.equals("fail"))
      .within(Time.seconds(10))
      //    对于已经创建好的 Pattern，可以指定循环次数，形成循环执行的 Pattern。
      //     times：可以通过 times 指定固定的循环执行次数。
      .times(1)

    // 检测Pattern
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(stream, pattern)
    //选择结果并输出
    val result: DataStream[String] = patternStream.select(new PatternSelectFunction[LoginEvent, String] {
      override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
        val keyIter: util.Iterator[String] = map.keySet().iterator()
        val e1: LoginEvent = map.get(keyIter.next()).iterator().next()
        val e2: LoginEvent = map.get(keyIter.next()).iterator().next()
        val e3: LoginEvent = map.get(keyIter.next()).iterator().next()
        "用户名:" + e1.userName + "登录时间:" + e1.eventTime + ":" + e2.eventTime + ":" + e3.eventTime
      }
    })
    result.print()
    env.execute()
  }
}
