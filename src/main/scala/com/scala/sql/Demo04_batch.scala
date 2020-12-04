package scala.com.scala.sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
 * 批次
 */
object Demo04_batch {
  def main(args: Array[String]): Unit = {
    //步骤：
    //批环境
    val benv = ExecutionEnvironment.getExecutionEnvironment

    //table环境
    val tenv = BatchTableEnvironment.create(benv)

    //通过socket接收实时到来的旅客信息，并封装成样例类
    import org.apache.flink.api.scala._
    val ds: DataSet[(String,Int)] = benv.readTextFile("E:\\flinkdata\\test.csv")
      .map(line=>{
        val fileds: Array[String] = line.split(",")
        (fileds(0).trim,fileds(1).toInt)
      })

    //基于DataSet生成一张Table
    import org.apache.flink.table.api.scala._
    val table: Table = tenv.fromDataSet(ds, 'name, 'age)

    //查询操作
    table
        .groupBy('name)
        .select('name,'age.sum as 'sum_age)
      .toDataSet[Row]
      .print()
  }
}
