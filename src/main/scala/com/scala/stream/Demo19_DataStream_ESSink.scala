package scala.com.scala.stream

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.com.scala.bean.Yq

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到ES中
 *
*/
object Demo19_DataStream_ESSink {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[Yq] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))
      .map(f=>{
        Yq(f._1.split("_")(0),f._1.split("_")(1),f._2._1,f._2._2)
      })

    sumed.print("adds&possibles->")
    try{
      //打入到ES中
      //es的连接信息
      val httpHosts = new util.ArrayList[HttpHost]
      httpHosts.add(new HttpHost("192.168.216.111", 9200, "http"))
      httpHosts.add(new HttpHost("192.168.216.112", 9200, "http"))
      httpHosts.add(new HttpHost("192.168.216.113", 9200, "http"))
      //获取es的sink
      val esSinkBuilder: ElasticsearchSink.Builder[Yq] = new ElasticsearchSink.Builder[Yq](httpHosts, new MyESSink)
      esSinkBuilder.setBulkFlushMaxActions(1)  //设置每一条数据刷新一次
      //esSinkBuilder.setBulkFlushInterval(1000)  //刷新数据的间隔

      //将essink添加到sink中即可
      sumed.addSink(esSinkBuilder.build())

    env.execute("es sink")
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}

/*
自定义es sink实现
 */
class MyESSink extends ElasticsearchSinkFunction[Yq] {
  //将数据写入到es中即可
  override def process(element: Yq,
                       ctx: RuntimeContext,
                       indexer: RequestIndexer): Unit = {
    try{
      print(s"统计信息：$element ")

      //i)将当前的user实例中的信息封装到Map中
      //dt: String, province: String, adds: int, possibles:Int
      val javamap = new util.HashMap[String,String]()
      //javamap.put("uid",element.uid.trim) //uid用于作文档id
      javamap.put("dt",element.dt.trim)
      javamap.put("age",element.province.toString)
      javamap.put("adds",element.adds+"")
      javamap.put("possibles",element.possibles+"")

      //ii)构建indexRequest实例
      val es_id:String = element.dt+"_"+element.province
      val indexRequest:IndexRequest = Requests.indexRequest()
        .index("yq_report")  //索引名称
        .`type`("info")  //索引的类型
        .id(es_id)  //类似主键
        .source(javamap)  //key-value值

      //iii)往es中添加数据信息
      indexer.add(indexRequest)
    } catch {
      case e1:Exception => e1.printStackTrace()
    }
  }
}