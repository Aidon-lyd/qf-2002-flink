package scala.com.scala.stream

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * reducefunction
 *
*/
object Demo33_DataStream_ReduceFunction {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .map(x => {
        val fields: Array[String] = x.split(" ")
        val date = fields(0).trim
        val province = fields(1)
        val add = fields(2).trim.toInt
        (date+"_"+province, add)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))  //每隔10秒
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          //比较找最大值
          if(value1._2>value2._2){
            value1
          } else{
            value2
          }

          //聚合
          //(value1._1,value1._2+value2._2)
        }
      })
      .print()

    env.execute("window")
  }
}

