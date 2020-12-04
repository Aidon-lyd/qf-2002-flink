package scala.com.scala.stream

import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer

/**
 * flink指带的source
 */
object Demo02_DataStreamSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1、集合source
    //创建一个集合
    val list: ListBuffer[Int] = new ListBuffer[Int]()
    list += 15
    list += 20
    list += 25

    //然后对list中的数据进行流式处理
    import org.apache.flink.api.scala._
    //集合作为源
    val dStream: DataStream[Int] = env.fromCollection(list)
    val filtered: DataStream[Int] = dStream.map(x => x * 100).filter(x => x >= 2000)
    //打印
    filtered.print("fromCollection->").setParallelism(1)
    println("============")

    //2、元素source
    val dStream1 = env.fromElements("flink is nice and flink nice") //字符串作为源
    dStream1.print("fromElements->").setParallelism(1)
    println("============")


    //3、文件source
    val dStream2 = env.readTextFile("E:\\flinkdata\\words.txt","utf-8")
    dStream2.print("readTextFile->").setParallelism(1)
    println("============")


    //4、读取hdfs文本文件，hdfs文件以hdfs://开头,不指定master的短URL
    val dStream3: DataStream[String] = env.readTextFile("hdfs://hadoop01:9000/words")
    dStream3.print("readTextFile->")

    //5、读取CSV文件,,,json格式自己读取即可
    val path = "E:\\flinkdata\\test.csv"
    val dStream4 = env.readTextFile(path)
    dStream4.print("readTextFile-csv->")

    //6、基于socket的sopurce
    val dStream6: DataStream[String] = env
        .socketTextStream("hadoop01", 6666)
        .setParallelism(1)  //不能设置为2及以上
    dStream6.print()

    //启动env
    env.execute("collection source")
  }
}
