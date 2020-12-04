package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 *操作链
 *
*/
object Demo23_DataStream_notchain {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //map操作符(调用startNewChain()操作符)不能往前链接，但是可能往后链接操作符----即map和print连接到一块
    import org.apache.flink.api.scala._
    env.fromElements("i like flink").map((_,1)).print("--startNewChain")

    //map操作符不能链接它的前面或者后面操作符---及禁止连接map操作
    env.fromElements("i like flink","i like flink").map((_,1)).print("--startNewChain")

    //将map操作放入指定slot组中共享slot，通常操作在默认slot中 ---即map共享default的slot
    env.fromElements("i like flink","i like flink","i like flink").map((_,1))
    //5、触发执行  流应用一定要触发执行
    env.execute("operter chain---")
  }
}