package com.it.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.it.flink.source.{MyCustomerSource, StationLog}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 随机生成stationLog对象，写入mysql数据库的表(t_station_log)中
 */
object CustomerJdbcSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[StationLog] = env.addSource(new MyCustomerSource)
    stream.addSink(new MyCustomerJdbcSink)
    env.execute()
  }
}

class MyCustomerJdbcSink extends RichSinkFunction[StationLog] {
  var conn: Connection = _
  var pst: PreparedStatement = _

  //生命周期管理，在Sink初始化的时候调用
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://node1/test", "root", "12345678")
    pst = conn.prepareStatement(
      "insert into t_station_log(sid, call_out, call_in, call_type, call_time, duration) " +
        "values(?, ?, ?, ?, ?, ?)")
  }

  override def invoke(value: StationLog, context: SinkFunction.Context[_]): Unit = {
    pst.setString(1, value.sid)
    pst.setString(2, value.callOut)
    pst.setString(3, value.callIn)
    pst.setString(4, value.callType)
    pst.setLong(5, value.callTime)
    pst.setLong(6, value.duration)
    pst.executeUpdate()
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}