package com.it.flink.cep

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.nfa.aftermatch.{AfterMatchSkipStrategy, SkipPastLastStrategy}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CepEvent {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements(
      Tuple3(1500, "login", "fail"),
      Tuple3(1500, "login", "fail"),
      Tuple3(1500, "login", "fail"),
      Tuple3(1320, "login", "success"),
      Tuple3(1450, "exit", "success"),
      Tuple3(982, "login", "fail"))

    val skipStrategy: SkipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent

    Pattern

  }
}
