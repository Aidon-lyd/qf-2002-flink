package scala.com.scala.stream

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/*
 * 广播变量
 *
*/
object Demo29_DataStream_Broadcast {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._


    val ds1: DataStream[(Int, String)] = env.fromElements((1, "男"), (2, "女"))
    //输入数据 uid uname genderflag address
    val ds2: DataStream[(Int, String, Int, String)] = env.socketTextStream("hadoop01", 6666).map(perLine => {
      val arr = perLine.split(" ")
      val id = arr(0).trim.toInt
      val name = arr(1).trim
      val genderFlg = arr(2).trim.toInt
      val address = arr(3).trim
      (id, name, genderFlg, address)
    })

    //广播变量描述器
    val desc = new MapStateDescriptor("genderInfo",
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    //广播出去
    val bc: BroadcastStream[(Int, String)] = ds1.broadcast(desc)

    //取出广播变量  输入数据的泛型 ,广播变量的类型 , 输出结果的类型
    val res: DataStream[(Int, String, String, String)] = ds2
      .connect(bc)
      .process(new BroadcastProcessFunction[(Int, String, Int, String), (Int, String), (Int, String, String, String)]() {

        //处理元素
        override def processElement(value: (Int, String, Int, String),
                                    ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, String), (Int, String, String, String)]#ReadOnlyContext,
                                    out: Collector[(Int, String, String, String)]): Unit = {
          //获取输入数据中genderFlag
          val genderFlg = value._3
          //使用上下文获取广播变量
          var gender = ctx.getBroadcastState(desc).get(genderFlg)
          //如果genderFlag取出来是Null，，需要单独处理一下
          if (gender == null) {
            gender = "妖"
          }
          //输出
          out.collect((value._1, value._2, gender, value._4))
        }

        override def processBroadcastElement(
                                              value: (Int, String),
                                              ctx: BroadcastProcessFunction[(Int, String, Int, String), (Int, String), (Int, String, String, String)]#Context,
                                              out: Collector[(Int, String, String, String)]): Unit = {
          ctx.getBroadcastState(desc).put(value._1, value._2)
        }
      })

    //打印
    res.print("broadcast->")

    env.execute("broad cast")
  }
}

