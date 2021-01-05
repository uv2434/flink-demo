package com.it.flink.sink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 把netcat作为数据源，统计单词数量并存入redis
 */
object RedisSink {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 8888)

    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setDatabase(3)
      .setHost("node3")
      .setPort(6379)
      .build()

    result.addSink(new RedisSink[(String, Int)](config, new RedisMapper[(String, Int)] {

      // 设置redis的命令
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "t_wc")
      }

      override def getKeyFromData(t: (String, Int)): String = {
        t._1
      }

      override def getValueFromData(t: (String, Int)): String = {
        t._2.toString
      }
    }))
    streamEnv.execute()
  }
}
