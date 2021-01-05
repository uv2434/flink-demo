package com.it.flink.transformation

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.it.flink.source.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 把通话成功的电话号码转化成真实的用户姓名，用户姓名保存在mysql数据库
 */
object TestRichFunctionClass {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val path: String = getClass.getResource("/station.log").getPath()
    val stream: DataStream[StationLog] = env.readTextFile(path)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })
    stream.filter(_.callType.equals("success"))
      .map(new MyRichMapFunction())
      .print()
    env.execute()

  }
}

class MyRichMapFunction extends RichMapFunction[StationLog, StationLog] {
  var conn: Connection = _
  var pst: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc://node1/test", "root", "12345678")
    pst = conn.prepareStatement("select name from t_phone where phone_number=?")
  }

  override def map(in: StationLog): StationLog = {
    //查询主叫用户的名字
    pst.setString(1, in.callOut)
    val set1: ResultSet = pst.executeQuery()
    if (set1.next()) {
      in.callOut = set1.getString(1)
    }
    //查询被叫用户的名字
    pst.setString(1, in.callIn)
    val set2: ResultSet = pst.executeQuery()
    if (set2.next()) {
      in.callIn = set2.getString(1)
    }
    in
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}