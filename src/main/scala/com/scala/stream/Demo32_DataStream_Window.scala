package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * 常见窗口操作
 *
*/
object Demo32_DataStream_Window {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      //.timeWindow(Time.seconds(5))  //基于时间滚动窗口
      //.timeWindow(Time.seconds(10),Time.seconds(5)) //基于时间的滑动窗口
      //.countWindow(3) //基于数据条数的滚动窗口
      //.countWindow(3,2) //基于数据条数的滑动窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
      .sum(1)
      .print("window")

    env.execute("window")
  }
}

