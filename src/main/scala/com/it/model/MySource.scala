package com.it.model

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * 自定义数据源
 */
class MySource extends RichSourceFunction[String] {

  var count = 1L
  var isRunning = true

   override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {

    while (isRunning) {
      //      val list: List[String] = List[String]()

      val buffer = new ListBuffer[String]()

      buffer.append("world")
      buffer.append("Flink")
      buffer.append("Table")
      buffer.append("SQL")
      buffer.append("hello")

      val i = Random.nextInt(buffer.size)
      sourceContext.collect(buffer.apply(i))

      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }

}
