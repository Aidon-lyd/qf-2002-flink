package scala.com.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/*
 * 和流式差不多，批次的分为3类：
 * 基于文件：
 * 基于集合：
 * 通用：
 */
object Demo02_basicsource {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    //读本地
    import org.apache.flink.api.scala._
    val localLines = env.readTextFile("E:\\flinkdata\\words.txt")
    localLines.print()

    //读hdfs
    val hdfsLines = env.readTextFile("hdfs://hadoop01:9000/words")
    hdfsLines.print()

    //读取csv文件
    val csvInput = env.readCsvFile[(String, Int)]("E:\\flinkdata\\test.csv")
    csvInput.print()

    //基于集合
    val colls = env.fromElements("Foo", "bar", "foobar", "fubar")
    colls.print()

    //生成数字序列
    val numbers = env.generateSequence(1, 100)
    numbers.print()

    //可以实现自定义输入的InputFormat，，然后使用readFile() | createInput()
    //env.createInput()
    //env.createInput()

    env.execute("batch source")
  }
}
