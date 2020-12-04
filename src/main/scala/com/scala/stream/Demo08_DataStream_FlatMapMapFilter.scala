package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.com.scala.bean.WordCount

/**
 * flink自定义
*/
object Demo08_DataStream_FlatMapMapFilter {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //转换统计之前必须进行隐式转换（引入整个scala的api包）
    import org.apache.flink.api.scala._
    val res: DataStream[WordCount] = dStream
      .flatMap(_.split(" "))
      .filter(x => x.length > 5) //长度大于5位的单词
      .map(w => WordCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(5))  //滑动窗口
      .sum("count")

    //sink地方
    res.print().setParallelism(1)

    //启动env
    env.execute("Map|FlatMap|Filter")
  }
}
