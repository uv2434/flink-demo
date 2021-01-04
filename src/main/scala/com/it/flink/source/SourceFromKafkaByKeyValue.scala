package com.it.flink.source

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

object SourceFromKafkaByKeyValue {
  def main(args: Array[String]): Unit = {

    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    properties.setProperty("group.id", "fink02")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("auto.offset.reset", "latest")

    val stream: DataStream[(String, String)] = streamEnv.addSource(
      new FlinkKafkaConsumer[(String, String)]("topic2", new MyKafkaReader, properties))
    stream.print()
    streamEnv.execute("SourceFromKafkaByKeyValue")
  }
}

class MyKafkaReader extends KafkaDeserializationSchema[(String, String)] {
  override def isEndOfStream(t: (String, String)): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
    if (consumerRecord == null) {
      return ("null", "null")
    }
    var key = ""
    var value = ""
    if (consumerRecord.key() != null) {
      key = new String(consumerRecord.key(), "UTF-8")
    }
    if (consumerRecord.value() != null) {
      value = new String(consumerRecord.value(), "UTF-8")
    }
    (key, value)
  }

  override def getProducedType: TypeInformation[(String, String)] = {
    createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
  }
}