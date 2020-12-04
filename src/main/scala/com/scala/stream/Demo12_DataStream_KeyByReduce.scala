package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
 * keyBy和reduce合并：
 *
 * keyBy:DataStream→KeyedStream
 * 将具有相同Keys的所有记录都分配给同一分区。内部用散列分区实现
 * keyby类似于sql中的group by，将数据进行了分组。后面基于keyedSteam的操作，都是组内操作。
 *
 * reduce：聚合
 * reduce表示将数据合并成一个新的数据，返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。
 * reduce方法不能直接应用于SingleOutputStreamOperator对象，因为这个对象是个无限的流，对无限的数据做合并，没有任何意义。
 * reduce需要针对分组或者一个window(窗口)来执行，也就是分别对应于keyBy、window/timeWindow 处理后的数据，
 * 根据ReduceFunction将元素与上一个reduce后的结果合并，产出合并之后的结果。
 */
object Demo12_DataStream_KeyByReduce {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    /*env.fromElements(Tuple2(200, 33), Tuple2(100, 65), Tuple2(100, 56), Tuple2(200, 666), Tuple2(100, 678))
      .keyBy(0)
      .reduce((a,b)=>(a._1,a._2+b._2))
      .print("keyBy Reduce->")*/

    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    dStream.map(line=>{
      val fields: Array[String] = line.split(",")
      val flag: Int = fields(2).toInt
      val age: Int = fields(3).toInt
      val local: String = fields(4).toString.trim
      //封装返回
      (local,(flag,age))
    })
      .filter(_._2._1 == 1)
        .keyBy(0)
        /*.reduce((a,b)=>{
          //累加新增数
          val adds = a._2._1 + b._2._1
          val ages = a._2._2 + b._2._2
          (a._1,(adds,ages/adds))
        })*/
      .sum(1)
        .print("news 19->")

    env.execute("reduce")
  }
}
