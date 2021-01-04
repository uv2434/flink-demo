package com.it.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * flink流计算，案例
 */
object FlinkStreamWordcount {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    // 默认并行度给所有算子适用，并行度<= slot数量
    streamEnv.setParallelism(1)
    // 2. 导入隐示转换
    import org.apache.flink.streaming.api.scala._
    // 3. 读取数据 sock流中的数据
    // DataStream 相当于spark中的Dstream
    val stream: DataStream[String] = streamEnv.socketTextStream("node1", 8888)

    // 4. 转换和处理数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1)).setParallelism(2)
      .keyBy(0) // 分组算子，0或者1 代表下标,0代表单词，1代表单词出现的次数
      .sum(1) // 聚会累加算子
    result.print("结果")
    streamEnv.execute("wordcount")
  }
}
