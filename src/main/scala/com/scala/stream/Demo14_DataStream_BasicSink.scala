package scala.com.scala.stream

import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
flink 的sink
*/
object Demo14_DataStream_BasicSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    val res: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //sink输出
    //res.writeAsText("E:\\flinkdata\\out\\00\\")
    //res.writeAsText("hdfs://hadoop01:9000/out/10")
    //csv只能使用数据为tuple
    //res.writeAsCsv("E:\\flinkdata\\out\\01\\",WriteMode.OVERWRITE,"\n",",")
    //res.writeAsCsv("E:\\flinkdata\\out\\03\\").setParallelism(1)
    //res.map(wc=>(wc._1,wc._2)).writeAsCsv("E:\\flinkdata\\out\\05\\").setParallelism(1)
    //打入socket
    //res.writeToSocket("hadoop01",9999,SerializableSerializer[(String,String)])
    res.writeToSocket("hadoop01",9999,new SerializationSchema[(String,Int)] {
      override def serialize(element: (String, Int)): Array[Byte] = element._1.getBytes
    })
    env.execute("sink")
  }
}
