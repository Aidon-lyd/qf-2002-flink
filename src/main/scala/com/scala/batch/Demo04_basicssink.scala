package scala.com.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/*
sink
 */
object Demo04_basicssink {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val text: DataSet[String] = env.fromElements("i like flink flink is nice","aa","aa")

    val res: AggregateDataSet[(String, Int)] = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //Text类型
    res.writeAsText("E:\\flinkdata\\out\\00") //本地或者是hdfs中的路径都可以
    res.writeAsText("E:\\flinkdata\\out\\00",WriteMode.OVERWRITE) //本地或者是hdfs中的路径都可以
    res.writeAsText("hdfs://hadoop01:9000/out/ds00",WriteMode.OVERWRITE)
    //CSV类型
    res.writeAsCsv("E:\\flinkdata\\out\\00csv")
    res.writeAsCsv("E:\\flinkdata\\out\\00csv","\n","|",WriteMode.OVERWRITE)

    //自定义OutputFormat类型：Write() 和 output()

  }
}
