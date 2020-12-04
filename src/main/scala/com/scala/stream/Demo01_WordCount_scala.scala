package com.qianfeg.stream

import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.taskexecutor.TaskExecutor
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 1、获取流式执行环境
 * 2、初始化数据源
 * 3、转换
 * 4、sink到目的地
 * 5、触发执行
 */
object Demo01_WordCount_scala {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、初始化数据
    val ds: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //3、转换
    import org.apache.flink.api.scala._
    val sumed: DataStream[(String, Int)] = ds
        //.filter()
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    //4、sink持久化
    sumed.print("wc->")
    //5、触发执行
    env.execute("flink scala wc ")

    /**
    //一行代码
    env.socketTextStream("hadoop01", 6666)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print("wc->")

    env.execute("flink scala wc ")
     */
  }
}
