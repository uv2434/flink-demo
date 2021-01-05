package com.it.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSinkByString {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 8888)

    val words: DataStream[String] = stream.flatMap(_.split(" "))

    words.addSink(new FlinkKafkaProducer[String]("node1:9092,node2:9092,node3:9092", "t_2020",
      new SimpleStringSchema()))
    streamEnv.execute("KafkaSinkByString")
  }
}
