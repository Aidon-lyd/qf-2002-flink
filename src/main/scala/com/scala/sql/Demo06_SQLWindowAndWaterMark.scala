package scala.com.scala.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * sql的窗口和水印
 */
object Demo06_SQLWindowAndWaterMark {
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
    var table: Table = tenv.fromDataStream(ds, 'dt,'province,'adds,'ts1.rowtime)

    //使用sql操作
    tenv.sqlQuery(
      s"""
        |select
        |province,
        |sum(adds)
        |from $table
        |group by province,tumble(ts1, interval '5' second)
        |""".stripMargin)
        .toAppendStream[Row]
        .print("sql")

    //启动
    env.execute("sql api")
  }
}
