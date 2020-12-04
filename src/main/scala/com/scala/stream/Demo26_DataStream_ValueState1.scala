package scala.com.scala.stream

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/*
 *valueState
 *
*/
object Demo26_DataStream_ValueState1 {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    dStream.filter(_.nonEmpty).map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Long = fields(2).toLong
      val possible: Long = fields(3).toLong
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .flatMap(new MyFlatMapFunction1)
      .print()

    //5、触发执行  流应用一定要触发执行
    env.execute("value state---")
  }
}

class MyFlatMapFunction1 extends RichFlatMapFunction[(String,(Long,Long)),(String,(Long,Long))]{
  /**
   * 初始化用于存储状态的熟悉，key：是count，value：是sum值
   */
  var sum:ValueState[(String,(Long,Long))] = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    //初始化valuestate描述器
    val descriptor: ValueStateDescriptor[(String,(Long,Long))] = new ValueStateDescriptor[(String,(Long,Long))](
      "average",  //描述器名称
      TypeInformation.of(new TypeHint[(String,(Long,Long))] {}),  //状态中的数据类型的类
      ("",(0l, 0l)))  //状态中的初始值
    //获取其状态
    sum = getRuntimeContext().getState(descriptor)
  }

  //关闭初始化资源
  override def close(): Unit = super.close()

  //核心逻辑实现
  override def flatMap(in: (String,(Long,Long)), out: Collector[(String,(Long,Long))]): Unit = {
    //获取当前状态
    var currentSum:(String,(Long,Long)) = sum.value()

    //count + 1
    //val count = currentSum._1  + 1
    //求sum
    val adds = currentSum._2._1 + in._2._1
    val possibles = currentSum._2._2 + in._2._2

    // 更新状态
    sum.update((in._1,(adds,possibles)))

    //输出状态
    out.collect(in._1,(adds,possibles))
  }
}

