package scala.com.scala.stream

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.flink.api.common.io.{InputFormat, OutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.kafka.clients.producer.ProducerRecord

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
 * 放到kafka中
 *
*/
object Demo17_DataStream_KafkaSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    try{
      //env.enableCheckpointing(10)
      val config: CheckpointConfig = env.getCheckpointConfig
      //config.enableExternalizedCheckpoints(CheckpointingMode.EXACTLY_ONCE)
      config.setCheckpointInterval(1000)
      config.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      config.setCheckpointTimeout(10000)


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

      //打入到kafka中
      //定义生产相关信息
      val to_topic = "flink_test"
      val prop: Properties = new Properties()
      prop.setProperty("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
      val kafkaproducer: FlinkKafkaProducer[Yq] = new FlinkKafkaProducer[Yq](to_topic, new MySerializationSchema(to_topic), prop, Semantic.EXACTLY_ONCE)
      sumed.addSink(kafkaproducer)

      env.execute("mysql outputformat")
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}

/*
自定义kafka序列化
 */
class MySerializationSchema(to_topic:String) extends KafkaSerializationSchema[Yq]{
  //序列化方法
  override def serialize(element: Yq, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //输出到kafka中的key为dt+province   value为整行值
    //返回
    new ProducerRecord[Array[Byte], Array[Byte]](to_topic,(element.dt+"_"+element.province).getBytes,
      ("{"+"dt"+":"+element.dt+","+"provice"+":"+element.province+","+"adds"+":"+element.adds+","+"possibles"+":"+element.possibles+"}").getBytes
    )
  }
}