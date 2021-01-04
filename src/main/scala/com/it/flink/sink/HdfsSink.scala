package com.it.flink.sink

import java.util.concurrent.TimeUnit

import com.it.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 每隔10s向hdfs中生成一条数据
 * 这个例子创建了一个简单的 Sink ，将记录分配给默认的一小时时间桶。它还指定了一个滚动策略，该策略在以下三种情况下滚动处于 In-progress 状态的部分文件（part file）：
 *
 * 它至少包含 15 分钟的数据
 * 最近 5 分钟没有收到新的记录
 * 文件大小达到 1GB （写入最后一条记录后）
 */
object HdfsSink {
  def main(args: Array[String]): Unit = {
    // 1. 初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    // 读取数据源
    val input: DataStream[StationLog] = streamEnv.addSource(new MyCustomerSource)

    // 默认一个小时一个目录（分桶）
    // 设置一个滚动策略
    val rollingPolicy: DefaultRollingPolicy[StationLog, String] = DefaultRollingPolicy.builder()
      .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //不活动的分桶等待时间,最近 5 分钟没有收到新的记录
      .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 每隔10秒生成一个文件
      .build()
    // 创建HDFS的sink

    val hdfsSink: StreamingFileSink[StationLog] = StreamingFileSink.forRowFormat[StationLog](
      new Path("hdfs://node1/temp/MySink001"),
      new SimpleStringEncoder[StationLog]("UTF-8")
    ).withRollingPolicy(rollingPolicy)
      .withBucketCheckInterval(1000) // 检查时间间隔
      .build()
    input.addSink(hdfsSink)
    streamEnv.execute()
  }
}
