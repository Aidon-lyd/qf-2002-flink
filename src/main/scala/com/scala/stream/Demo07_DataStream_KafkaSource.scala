package scala.com.scala.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * flink自定义
*/
object Demo07_DataStream_KafkaSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.enableCheckpointing(10)
    //配置kafka的相关消费信息
    val from_topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("group.id", "test-group")
    import org.apache.flink.api.scala._
    /*val flinkConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      from_topic,
      new SimpleStringSchema(),
      properties)*/
    val flinkConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      from_topic,
      new MyStringDes(),
      properties)
    //对消费者进行设置
    //flinkConsumer.setStartFromSpecificOffsets()  //指定分区，指定offset进行消费
/*    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition(from_topic, 0), 23L)
    specificStartOffsets.put(new KafkaTopicPartition(from_topic, 1), 31L)
    specificStartOffsets.put(new KafkaTopicPartition(from_topic, 2), 43L)
    flinkConsumer.setStartFromSpecificOffsets(specificStartOffsets)*/

    flinkConsumer.setStartFromLatest() //消费最新数据

    //正则指定分区
    /*val myConsumer = new FlinkKafkaConsumer[String](java.util.regex.Pattern.compile("test-[0-9]"),
      new SimpleStringSchema,
      properties)*/
    /**
     * 某设备连续异常3告警
     * deviceID exception_code 时间(天时分秒)
     *
     * 一个文件中；设置并行度2
     * 100      201    2020-10-12 11:56：3
     * 100      201    2020-10-12 11:56：4
     * 100      201    2020-10-12 11:56：7
     * 102      202
     * 100      201    2020-10-12 11:56：9
     * 100      201    2020-10-12 11:56：1
     *
     *
     *将相同的设备数据打入相同的kafka的partition中。再设置并行度为分区个数的并行度
     */
    val ds = env.addSource(flinkConsumer).setParallelism(5)

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

//自定义kafka的消费时的反序列化  --- 泛型，该序列化返回类型
class MyStringDes extends KafkaDeserializationSchema[String] {
  override def isEndOfStream(nextElement: String): Boolean = false

  //反序列化的实现  ConsumerRecord ： key-value
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): String = {
    //也可以做数据的转换清洗等
    new String(record.value())+"_shcema"
  }

  //获取生产类型
  override def getProducedType: TypeInformation[String] = {
    TypeInformation.of(classOf[String])
  }
}
