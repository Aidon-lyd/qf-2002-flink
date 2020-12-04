package scala.com.scala.stream

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 * 累加器
 *
*/
object Demo31_DataStream_Accumulator {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //1、分布式缓存文件到hdfs中  1,男
    env.registerCachedFile("hdfs://hadoop01:9000/sex.txt","gender")

    //输入数据 uid uname genderflag address
    env.socketTextStream("hadoop01", 6666)
    .map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, add, possible)
    })
      .keyBy(0)
      .map(new RichMapFunction[(String,Int,Int),(String,Int,Int)] {
        //定义累加器
        val adds = new IntCounter(0)  //0是初始值
        val possibles = new IntCounter(0)

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator("adds", adds)
          getRuntimeContext.addAccumulator("possibles", possibles)
        }

        //处理每一行输入数据
        override def map(value: (String, Int, Int)): (String, Int, Int) = {
          adds.add(value._2)  //累加新增
          possibles.add(value._3)
          //构造返回
          (value._1,adds.getLocalValue,possibles.getLocalValue)
        }

        override def close(): Unit = super.close()
      })
        .print("acc")


    //触发执行
    val result: JobExecutionResult = env.execute("distributed cache")
    val adds: AnyRef = result.getAllAccumulatorResults.get("adds")
    val possibles: AnyRef = result.getAllAccumulatorResults.get("possibles")
    println(s"累计新增:${adds},累计怀疑:${possibles}")
  }
}

