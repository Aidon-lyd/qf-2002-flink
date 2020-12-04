package scala.com.scala.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

/*
聚合：
 * 聚合算子：KeyedStream→DataStream
 * min
 * minBy
 * max
 * maxBy
 * sum
*/
object Demo13_DataStream_Aggregation {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    val ks: KeyedStream[(Int, Int), Tuple] =
      env.fromElements(Tuple2(200, 33), Tuple2(100, 65), Tuple2(100, 56), Tuple2(200, 666), Tuple2(100, 678))
      .keyBy(0)

    //聚合函数
    //ks.max(1).print("max->")
    //ks.maxBy(1).print("maxBy->")
    ks.min(1).print("min->")
    ks.minBy(1).print("minBy->")
    //ks.sum(1).print("sum->")

    env.execute("reduce")
  }
}
