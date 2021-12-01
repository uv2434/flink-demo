package com.it.flink.transformation

import com.it.flink.source.StationLog
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 把呼叫成功的Stream（主流）和不成功的Stream（侧流）分别输出。
 *
 * 在处理一个数据源时，往往需要将该源中的不同类型的数据做分割处理，
 * 如果使用 filter 算子对数据源进行筛选分割的
 * 话，势必会造成数据流的多次复制，造成不必要的性能浪费；flink 中的侧输出就是将数据
 * 流进行分割，而不对流进行复制的一种分流机制。flink 的侧输出的另一个作用就是对延时
 * 迟到的数据进行处理，这样就可以不必丢弃迟到的数据。
 */
object TestSideOutputStream {
  //侧输出流首先需要定义一个流的标签
  var notSuccessTag = new OutputTag[StationLog]("not_success")

  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取文件数据
    val data = env.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        var arr = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val mainStream: DataStream[StationLog] = data.process(new CreateSideOutputStream(notSuccessTag))
    mainStream.print("主流输出：")
    mainStream.getSideOutput(notSuccessTag)
      .print("测流输出")

    env.execute()
  }
}

class CreateSideOutputStream(tag: OutputTag[StationLog]) extends ProcessFunction[StationLog, StationLog] {
  override def processElement(value: StationLog, ctx: ProcessFunction[StationLog, StationLog]#Context, out: Collector[StationLog]): Unit = {
    if (value.callType.equals("success")) { //输出主流
      out.collect(value)
    } else { //输出侧流
      ctx.output(tag, value)
    }
  }
}