package scala.com.scala.stream

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/*
 * 分布式缓存
 *
*/
object Demo30_DataStream_DistributedCache {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //1、分布式缓存文件到hdfs中  1,男
    env.registerCachedFile("hdfs://hadoop01:9000/sex.txt","gender")

    //输入数据 uid uname genderflag address
    val ds2: DataStream[(Int, String, Int, String)] = env.socketTextStream("hadoop01", 6666).map(perLine => {
      val arr = perLine.split(" ")
      val id = arr(0).trim.toInt
      val name = arr(1).trim
      val genderFlg = arr(2).trim.toInt
      val address = arr(3).trim
      (id, name, genderFlg, address)
    })

    //使用自定义函数获取分布式缓存文件即可
    val res: DataStream[(Int, String, String, String)] = ds2.map(new RichMapFunction[(Int, String, Int, String), (Int, String, String, String)] {

      //可变的Map集合，用于存放从分布式缓存中读取的信息
      val map: mutable.Map[Int, String] = mutable.HashMap()

      //存储分布式缓存中的数据流
      var bs: BufferedSource = _

      override def open(parameters: Configuration): Unit = {
        val genderFile: File = getRuntimeContext.getDistributedCache.getFile("gender")
        //将读取到的信息封装到Map实例中存储起来
        bs = Source.fromFile(genderFile)
        val lst = bs.getLines().toList
        for (perLine <- lst) {
          val arr = perLine.split(",")
          val genderFlg = arr(0).trim.toInt
          val genderName = arr(1).trim
          map.put(genderFlg, genderName)
        }
      }

      //处理每一行数据
      override def map(value: (Int, String, Int, String)): (Int, String, String, String) = {
        //从输入的value中获取genderFlag
        val genderFlag: Int = value._3
        //判断map中是否有存在
        val gender: String = map.getOrElse(genderFlag, "妖")
        (value._1, value._2, gender, value._4)
      }


      override def close(): Unit = {
        //关闭文件
        if (bs != null)
          bs.close()
      }
    })

    //打印
    res.print("distributed cache->")

    env.execute("distributed cache")
  }
}

