package scala.com.scala.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * flink自定义
*/
object Demo06_DataStream_KafkaSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置kafka的相关消费信息
    val from_topic = "test"
    val properties = new Properties()
    //properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("bootstrap.servers", "hadoop01:9092")
    properties.setProperty("group.id", "test-group")
    import org.apache.flink.api.scala._
    val ds = env.addSource(new FlinkKafkaConsumer[String](from_topic, new SimpleStringSchema(), properties))

    //操作
    val fitered: DataStream[String] = ds.filter(_.length >= 5)
    /*ds.map(par=>{
      //复杂的实时的清洗
    })*/
    fitered.print("kafka-connector->")

    //启动env
    env.execute("kafka connector")
  }
}
