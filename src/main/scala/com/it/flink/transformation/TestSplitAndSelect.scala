package com.it.flink.transformation

import com.it.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

object TestSplitAndSelect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[StationLog] = env.addSource(new MyCustomerSource)
    val splitStream: SplitStream[StationLog] = stream.split(log => {
      if (log.callType.equals("success")) Seq("Success") else Seq("No success")
    })
    val result1: DataStream[StationLog] = splitStream.select("Success")
    val result2: DataStream[StationLog] = splitStream.select("No success")

    result1.print("通话成功")
    result2.print("通话不成功")
    env.execute()
  }
}
