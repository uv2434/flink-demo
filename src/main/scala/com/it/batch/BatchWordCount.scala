package com.it.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet, createTypeInformation}

object BatchWordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.fromElements("Flink Batch", "batch demo", "demo", "demo")

    val ds = text.flatMap(_.split("\\W+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    ds.print()
  }
}
