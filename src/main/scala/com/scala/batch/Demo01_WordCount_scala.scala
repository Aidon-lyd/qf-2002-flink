package com.qianfeg.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * scala的版本的批次案例
 * 步骤：
 * 1、获取批次的执行环境
 * 2、初始化数据源
 * 3、转换
 * 4、sink
 */
object Demo01_WordCount_scala {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    import org.apache.flink.api.scala._
    env.fromElements("i like flink","flink is nice","flink nice")
      .flatMap(_.split(" "))
      .filter(_.size>=2)
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
