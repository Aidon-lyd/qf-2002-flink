package scala.com.scala.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * sql统计
 */
object Demo07_wordcount {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //table环境
    val tenv = StreamTableEnvironment.create(env)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .flatMap(_.split(" "))
      .map((_, 1))


    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataStream(ds, 'word,'cnt)

    //使用sql操作
    tenv.sqlQuery(
      s"""
        |select
        |word,
        |sum(cnt)
        |from $table
        |group by word
        |""".stripMargin)
        .toRetractStream[Row]
        .print("sql")

    //启动
    env.execute("sql wordcount")
  }
}
