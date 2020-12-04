package scala.com.scala.stream

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{BasePathBucketAssigner, DateTimeBucketAssigner}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.com.scala.bean.{YQDetail, Yq}

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 放到ES中
 *
*/
object Demo20_DataStream_StreamingFileSink {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    //实时落地一定需要开启检测点；；如果不开启，不会报错，但是数据永远不会回滚
    env.enableCheckpointing(5*1000)

    try{
    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._


     //2020-5-13 beijing 1 2
   val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
      /*
      val ds: DataStream[String] = dStream.map(line => {
        val fields: Array[String] = line.split(" ")
        val dt: String = fields(0).toString.trim
        val province: String = fields(1).toString.trim
        val add: Int = fields(2).toInt
        val possible: Int = fields(3).toInt
        //封装返回
        (dt + "_" + province, (add, possible))
      })
        .filter(f => {
          f._2._1 != 0 && f._2._2 != 0 //过滤
        })
        .map(x => {
          x._1.split("_")(0) + "," + x._1.split("_")(1) + "," + x._2._1 + "," + x._2._2
        })

      ds.print("adds&possibles->")


      //定义文件实时落地
      //数据落地路径
      val outputPath :Path = new Path("hdfs://hadoop01:9000/out/flink/yq")
      //落地策略
      val rollingPolicy :DefaultRollingPolicy[String,String] = DefaultRollingPolicy.create()
        .withRolloverInterval(10*1000)  //回滚落地间隔，，单位毫秒
        .withInactivityInterval(5*1000)   //无数据时间间隔
        .withMaxPartSize(128*1024*1024)  //最大文件数量限制,,默认字节
        .build()

            //数据分桶分配器  trave/orders/dt=/2020061909  --- 如果数据目录不想有时间可以使用BasicPathBucketAssigner
            val bucketAssigner :BucketAssigner[String,String] = new DateTimeBucketAssigner("yyyyMMddHH")

           //输出sink
            val hdfsSink: StreamingFileSink[String] = StreamingFileSink
              //forRowFormat --- 行编码格式
              //forBulkFormat(outputBasePath, ParquetAvroWriters.forGenericRecord(schema))
              .forRowFormat(outputPath, new SimpleStringEncoder[String]("UTF-8"))
              .withBucketAssigner(bucketAssigner)  //设置桶分配器
              .withRollingPolicy(rollingPolicy)  //设置回滚策略
              .withBucketCheckInterval(5*1000)  //桶检测间隔
              .build()
            //添加sink
            ds.addSink(hdfsSink)*/

      //使用parquert格式落地
      val outputPath :Path = new Path("hdfs://hadoop01:9000/out/flink/yq_parquet")
      val ds1: DataStream[YQDetail] = dStream.map(line => {
        val fields: Array[String] = line.split(" ")
        val dt: String = fields(0).toString.trim
        val province: String = fields(1).toString.trim
        val add: Int = fields(2).toInt
        val possible: Int = fields(3).toInt
        //封装返回
        (dt + "_" + province, (add, possible))
      })
        .filter(f => {
          f._2._1 != 0 && f._2._2 != 0 //过滤
        })
        .map(x => {
          YQDetail(x._1.split("_")(0) , x._1.split("_")(1) , x._2._1 , x._2._2)
        })

      ds1.print()

      //数据分桶分配器
      val bucketAssigner :BucketAssigner[YQDetail,String] = new DateTimeBucketAssigner("yyyyMMddHH")
      //val bucketAssigner: BasePathBucketAssigner[YQDetail] = new BasePathBucketAssigner()
      //4 数据实时采集落地
      //需要引入Flink-parquet的依赖
      val hdfsParquetSink: StreamingFileSink[YQDetail] = StreamingFileSink

        //块编码
        .forBulkFormat(
          outputPath,
          ParquetAvroWriters.forReflectRecord(classOf[YQDetail])) //paquet序列化
        .withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(5*1000) //分桶器检测间隔
        .build()

      //添加sink
      ds1.addSink(hdfsParquetSink)

    env.execute("Streaming File sink")
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}