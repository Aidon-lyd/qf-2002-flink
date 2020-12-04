package scala.com.scala.stream

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * aggregatefunction
 *
*/
object Demo34_DataStream_AggFunction {
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
      // 输入数据类型  累加器的类型  返回类型
      .aggregate(new AggregateFunction[(String,Int),(String,Int,Int),(String,Int)] {
        //初始化
        override def createAccumulator(): (String, Int, Int) = ("",0,0)
        //单个增加
        override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
          val cnt = accumulator._2 + 1
          val adds = accumulator._3 + value._2
          (value._1, cnt, adds)
        }

        //多个分区中的进行合并
        override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
          val mergeCnt = a._2 + b._2
          val mergeAdds = a._3 + b._3
          (a._1, mergeCnt, mergeAdds)
        }

        //获取结果
        override def getResult(accumulator: (String, Int, Int)): (String, Int) = {
          (accumulator._1, accumulator._3 / accumulator._2)
        }
      })
      .print()

    env.execute("window")
  }
}

