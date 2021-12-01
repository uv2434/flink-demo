package com.it.flink.sink

import java.lang
import java.util.Properties

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * kafka作为sink的第二种 (KV)
 */
object KafkaSinkByKeyValue {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 8888)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")

    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    result.addSink(new FlinkKafkaProducer[(String, Int)](
      "t_2020",
      new KafkaSerializationSchema[(String, Int)] {
        override def serialize(t: (String, Int), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("t_2020", t._1.getBytes(), t._2.toString.getBytes())
        }
      }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    streamEnv.execute()
  }
}
