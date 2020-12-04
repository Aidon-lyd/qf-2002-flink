package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/*
 * aggregatefunction
 *
*/
object Demo36_DataStream_Trigger {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val res: DataStream[(String, Int)] = env.socketTextStream("hadoop01", 6666)
      .map(x => {
        val fields: Array[String] = x.split(" ")
        val date = fields(0).trim
        val province = fields(1)
        val add = fields(2).trim.toInt
        (date + "_" + province, add)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(20)) //每隔10秒
      //输出数据类型    构造器中的泛型：输入类型   输出类型  key的类型  窗口类型
      .trigger(new MyTrigger)
      .sum(1)

      res.print("trigger->")

    env.execute("window")
  }
}

//自定义触发器
class MyTrigger extends Trigger[(String,Int),TimeWindow]{

  var cnt = 0
  //每一行数据的处理逻辑
  override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //注册一个时间触发器;;;当当前时间超过window.maxTimestamp()就调用onProcessingTime()方法
    ctx.registerProcessingTimeTimer(window.maxTimestamp())  //window.maxTimestamp()：当前窗口最大值，窗口大小不一样，最大值不一样
    //ctx.registerEventTimeTimer()
    println("指定时间:"+window.maxTimestamp())
    //判断数据条数是否大于5条，大于就触发，然后cnt重置0
    if (cnt > 5) {
      println("通过计数触发窗口...")
      cnt = 0
      TriggerResult.FIRE  //触发窗口执行
    } else {
      cnt = cnt + 1
      TriggerResult.CONTINUE //什么都不做。继续
    }
  }

  //基于处理时间触发操作
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    println("通过处理时间触发窗口...")
    TriggerResult.FIRE   //基于处理时间触发
  }

  //基于事件时间触发的操作
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = ???

  //清空窗口数据操作
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteProcessingTimeTimer(window.maxTimestamp()) //删除基于处理时间的触发器
  }
}

