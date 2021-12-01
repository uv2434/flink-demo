package com.it.flink.source

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object KafkaProducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.setProperty("key.serializer", classOf[StringSerializer].getName)
    props.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    producer.send(
      new ProducerRecord[String, String](
        "topic2", "key2", "value2"))
    producer.close()
  }
}
