package scala.com.scala.stream

import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.streaming.api.graph.{StreamGraph, StreamingJobGraphGenerator}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/*
 *并行度
 *
*/
object Demo21_DataStream_Parallel {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、获取source
    import org.apache.flink.api.scala._
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //3、基于source的transformation
    //具体转换
    val maped: DataStream[(String, Int)] = dStream.flatMap(_.split(" "))
      .map(word => (word, 1))

    //基于算子设置并行--针对该操作链
    maped.setParallelism(8)

    val sumed: DataStream[(String, Int)] = maped.keyBy(0)
      .timeWindow(Time.seconds(5), Time.seconds(5))
      .sum(1)
    //基于算子设置并行--针对该操作链
    sumed.setParallelism(4)
    sumed.setMaxParallelism(5)  //设置最大并行度

    //4、结果sink --基于算子设置并行
    sumed.print().setParallelism(2)

    env.execute("parallel")
  }
}