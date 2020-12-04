package scala.com.scala.stream

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/*
 *valueState
 *
*/
object Demo25_DataStream_ValueState {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromElements((1L, 5L), (1L, 6L), (1L, 10L), (1L, 100L),(2L, 3L), (2L, 8L))
      .keyBy(0)
      .flatMap(new MyFlatMapFunction())
      .print()

    //5、触发执行  流应用一定要触发执行
    env.execute("value state---")
  }
}

class MyFlatMapFunction extends RichFlatMapFunction[(Long,Long),(Long,Long)]{
  /**
   * 初始化用于存储状态的熟悉，key：是count，value：是sum值
   */
  var sum:ValueState[(Long,Long)] = _

  //初始化
  override def open(parameters: Configuration): Unit = {
    //初始化valuestate描述器
    val descriptor: ValueStateDescriptor[(Long, Long)] = new ValueStateDescriptor[(Long, Long)](
      "average",  //描述器名称
      TypeInformation.of(new TypeHint[(Long, Long)] {}),  //状态中的数据类型的类
      (0l, 0l))  //状态中的初始值
    //获取其状态
    sum = getRuntimeContext().getState(descriptor)
  }

  //关闭初始化资源
  override def close(): Unit = super.close()

  //核心逻辑实现
  override def flatMap(in: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    //获取当前状态
    var currentSum: (Long, Long) = sum.value()

    //count + 1
    val count = currentSum._1  + 1
    //求sum
    val sumed = currentSum._2 + in._2

    // 更新状态
    sum.update((count,sumed))

    //输出状态
    //out.collect(in._1,sumed)

    //状态输出：如果count到达2, 保存 count和平均值，清除之前的状态
    if (sum.value()._1 >= 2) {
      //out.collect((in._1, sum.value()._2 / sum.value()._1))
      out.collect((in._1, sum.value()._2))
      //状态清空
      sum.clear()
    }
  }
}

