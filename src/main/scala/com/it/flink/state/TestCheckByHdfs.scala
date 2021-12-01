package com.it.flink.state

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TestCheckByHdfs {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStateBackend(new FsStateBackend("hdfs://node2/flink/checkpoint/cp1"))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(5000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) // 终止job时不会删除检查点数据

    // 默认并行度给所有算子适用，并行度<= slot数量
    env.setParallelism(1)

    // 3. 读取数据 sock流中的数据
    // DataStream 相当于spark中的Dstream
    val stream: DataStream[String] = env.socketTextStream("node1", 8888)
    // 4. 转换和处理数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .uid("flatMap001")
      .map((_, 1)).uid("map001")
      .keyBy(0) // 分组算子，0或者1 代表下标,0代表单词，1代表单词出现的次数
      .sum(1).uid("sum001") // 聚会累加算子
    result.print("结果")
    env.execute("wordcount")
    2.12
    .7
  }
}
