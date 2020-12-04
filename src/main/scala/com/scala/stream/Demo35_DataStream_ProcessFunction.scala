package scala.com.scala.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * aggregatefunction
 *
*/
object Demo35_DataStream_ProcessFunction {
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
      //输出数据类型    构造器中的泛型：输入类型   输出类型  key的类型  窗口类型
      .process[(String, Double)](new ProcessWindowFunction[(String,Int), (String, Double), Tuple, TimeWindow] {
        //处理窗口中的每一行元素  --- 输入数据是一个迭代器
        override def process(key: Tuple,
                             context: Context,
                             elements: Iterable[(String,Int)],
                             out: Collector[(String, Double)]): Unit = {
          //计算平均值
          var cnt = 0
          var totalAdds = 0.0
          //循环累加
          elements.foreach(perRecord => {
            cnt = cnt + 1
            totalAdds = totalAdds + perRecord._2
          })
          //输出
          out.collect((key.getField(0), totalAdds / cnt))
        }
      }).print()

    env.execute("window")
  }
}

