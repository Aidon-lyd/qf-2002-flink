package scala.com.scala.stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.com.scala.bean.Temp

/**
 * flink自定义
 *
 * sideOutput:
 * 1、先定义outputTag，即侧输标识
 * 2、使用process函数进行数据流输出
 * 3、使用getSideOutput获取侧输流
*/
object Demo10_DataStream_SideOutput {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取流数据
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)

    //将温度正常和异常的旅客拆分
    //输入数据：uid temp uname timestamp location
    import org.apache.flink.api.scala._
    val ds: DataStream[Temp] = dStream.map(perInfo => {
      val arr = perInfo.split(",")
      val uid: String = arr(0).trim
      val temperature: Double = arr(1).trim.toDouble
      val uname: String = arr(2).trim
      val timestamp: Long = arr(3).trim.toLong
      val location: String = arr(4).trim
      Temp(uid, temperature, uname, timestamp, location)
    })

    //初始化侧输标识
    val normalTag = OutputTag[String]("normal")  //正常标识
    val exceptionTag = OutputTag[String]("exception")  //正常标识

    //数据流处理
    val outputSideDS: DataStream[String] = ds.process(new ProcessFunction[Temp, String] {
      override def processElement(value: Temp,
                                  ctx: ProcessFunction[Temp, String]#Context,
                                  out: Collector[String]): Unit = {
        // emit data to regular output
        if (value.temp < 37.0 && value.temp > 35.3) {
          //out.collect(value.uid + "_" + value.temp)
          // emit data to side output
          ctx.output(normalTag, "normal-" + String.valueOf(value))
        } else {
          //out.collect(value.uid + "_" + value.temp)
          ctx.output(exceptionTag, "exception-" + String.valueOf(value))
        }

      }
    })

    //取出侧输流
    outputSideDS.getSideOutput(normalTag).print("normal->")
    outputSideDS.getSideOutput(exceptionTag).print("execption->")

    //启动env
    env.execute("sideoutput")
  }
}
