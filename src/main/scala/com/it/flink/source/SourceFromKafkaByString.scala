package com.it.flink.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * kafka中的数据是普通字符串
 */
object SourceFromKafkaByString {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.setProperty("group.id", "fink01")
    props.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset", "latest")

    val consumer = new FlinkKafkaConsumer[String]("topic2", new SimpleStringSchema(), props)

    val stream: DataStream[String] = streamEnv.addSource(consumer)
    stream.print()
    streamEnv.execute("SourceFromKafka")
  }
}
