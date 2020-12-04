package scala.com.scala.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * 窗口和水印
 */
object Demo03_WindowAndWaterMark {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //table环境
    val tenv = StreamTableEnvironment.create(env)


    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[Yq] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        val date: String = arr(0).trim
        val province: String = arr(1).trim
        val add: Int = arr(2).trim.toInt
        val possible: Int = arr(2).trim.toInt
        Yq(date, province, add, possible)
      }) //下边是分配水印和时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Yq](Time.seconds(2)) {
        override def extractTimestamp(element: Yq): Long = element.dt.toLong * 1000
      })

    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    var table: Table = tenv.fromDataStream(ds, 'd,'p,'a)

    //查询table中特定的字段  ,,,如果是样例类，可以使用字段:table.select("date,province,add")
    //table = table.select("_1,_2,_3")
    table = tenv.fromDataStream(ds, 'dt, 'add,'ts.rowtime)
      .window(Tumble over 5.second on 'ts as 'tt1)
      .groupBy('dt, 'tt1)
      .select('dt, 'dt.count,'add.sum)

    //将table中的数据拿到新的DataStream中，然后输出
    tenv.toAppendStream[Row](table)
      .print("表中输出输出后的结果是 →")

    //启动
    env.execute("table api")
  }
}
