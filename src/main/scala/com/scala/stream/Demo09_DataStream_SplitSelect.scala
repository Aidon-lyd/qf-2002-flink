package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.com.scala.bean.{Temp, WordCount}

/**
 * flink自定义
 *
 *   Select应用于Split的结果
 * * split:DataStream→ SplitStream
 * * 将指定的DataStream拆分成多个流用SplitStream来表示
 * *
 * * select:SplitStream→ DataStream
 * * 跟split搭配使用，从SplitStream中选择一个或多个流
*/
object Demo09_DataStream_SplitSelect {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //将温度正常和异常的旅客拆分
    //输入数据：uid temp uname timestamp location
    import org.apache.flink.api.scala._
    val ss: SplitStream[Temp] = dStream.map(perInfo => {
      val arr = perInfo.split(",")
      val uid: String = arr(0).trim
      val temperature: Double = arr(1).trim.toDouble
      val uname: String = arr(2).trim
      val timestamp: Long = arr(3).trim.toLong
      val location: String = arr(4).trim
      Temp(uid, temperature, uname, timestamp, location)
    })
      .split((temp: Temp) => {
        if (temp.temp >= 35.9 && temp.temp <= 37.5)
          Seq("正常")  //一个数据流的标识
        else
          Seq("异常")
      }
      )


    ss.print("split Stream->")

    //使用Select来选择流
    ss.select("正常").print("正常旅客->")
    ss.select("异常").print("异常旅客->")

    //启动env
    env.execute("Split和Select")
  }
}
