package scala.com.scala.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

import scala.com.scala.bean.Yq

/**
 * sql
 */
object Demo05_sql {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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
        val possible: Int = arr(3).trim.toInt
        Yq(date, province, add, possible)
      })

    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataStream(ds)

    //duisql进行查询
   /* tenv.sqlQuery(
      s"""
        |select
        |*
        |from $table
        |where adds > 5
        |""".stripMargin)
        .toAppendStream[Row]
        .print("sql->")*/

    tenv.sqlQuery(
      s"""
         |select
         |dt,
         |sum(adds) adds,
         |sum(possibles) possibles
         |from $table
         |group by dt
         |""".stripMargin)
      //.toAppendStream[Row]
      .toRetractStream[Row]
      .print("sql->")

    //启动
    env.execute("sql api")
  }
}
