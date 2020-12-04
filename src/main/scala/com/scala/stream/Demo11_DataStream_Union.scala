package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, SplitStream, StreamExecutionEnvironment}

import scala.com.scala.bean.Temp

/*
 * DataStream的算子：
 * union和connect：合并流
 *
 * union：DataStream* → DataStream
 * union 合并多个流，新的流包含所有流的数据；union的多个子流类型一致
 *
 * connect：* -> ConnectedStreams
 * connect只能连接两个流;onnect连接的两个流类型可以不一致;
 * 两个流之间可以共享状态(比如计数)。这在第一个流的输入会影响第二个流时, 会非常有用
 */
object Demo11_DataStream_Union {
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

    /*//使用Select来选择流
    val commonDS: DataStream[Temp] = ss.select("正常")
    val execeptionDS: DataStream[Temp] = ss.select("异常")
    //union合并流
    val unionDS: DataStream[Temp] = commonDS.union(execeptionDS)
    unionDS.print("unionDS->")
    */

    val commonDS: DataStream[(String, Double)] = ss.select("正常").map(f => (f.uid, f.temp))
    val execeptionDS: DataStream[(String, Double, String)] = ss.select("异常").map(f => (f.uid, f.temp, f.uname))

    //union合并流
    //val unionDS: DataStream[Temp] = commonDS.union(execeptionDS)
    //unionDS.print("unionDS->")

    //connect合并流
    val connectDS: ConnectedStreams[(String, Double), (String, Double, String)] = commonDS.connect(execeptionDS)
    val connectMapDS: DataStream[String] = connectDS.map(
      common => "正常： " + common._1 + "_" + common._2,
      exception => "异常： " + exception._1 + "_" + exception._2 + "_" + exception._3)
    connectMapDS.print("connectDS->")

    env.execute("unionDS")
  }
}
