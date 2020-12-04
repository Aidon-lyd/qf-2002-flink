package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 *分区器
 *
*/
object Demo24_DataStream_partitioner {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dstream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    //shuffle ： 混乱分区，是随机发送
    dstream.shuffle.print("shuffle->").setParallelism(4)
    //reblanace ： 使用轮询发送下游
    dstream.rebalance.print("rebalance->").setParallelism(4)
    //resscala
    dstream.rescale.print("rescala->").setParallelism(4)

    //5、触发执行  流应用一定要触发执行
    env.execute("operter partitioner---")
  }
}