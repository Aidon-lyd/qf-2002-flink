package scala.com.scala.sql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * 别名
 */
object Demo02_alias {
  def main(args: Array[String]): Unit = {
    //步骤：
    //流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //table环境
    val tenv = StreamTableEnvironment.create(env)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataStream[(String, String, Int, Int)] = env.socketTextStream("hadoop01", 6666)
      .filter(_.trim.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        val date: String = arr(0).trim
        val province: String = arr(1).trim
        val add: Int = arr(2).trim.toInt
        val possible: Int = arr(2).trim.toInt
        (date, province, add, possible)
      })

    //基于DataStream生成一张Table
    import org.apache.flink.table.api.scala._
    var table: Table = tenv.fromDataStream(ds, 'd,'p,'a)

    //查询table中特定的字段  ,,,如果是样例类，可以使用字段:table.select("date,province,add")
    //table = table.select("_1,_2,_3")
    table = table
      .select('d,'p,'a)
      //  .select("d,p,a")  //等价于上边
      .where("a>5")

    //将table中的数据拿到新的DataStream中，然后输出
    tenv.toAppendStream[Row](table)
      .print("表中输出输出后的结果是 →")

    //启动
    env.execute("table api")
  }
}
