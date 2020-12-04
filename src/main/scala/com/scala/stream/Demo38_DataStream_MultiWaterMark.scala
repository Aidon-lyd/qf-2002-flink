package scala.com.scala.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * 多并行度下的水印
 *
*/
object Demo38_DataStream_MultiWaterMark {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    //输入数据两列  name 时间戳
    val res: DataStream[String] = env.socketTextStream("hadoop01", 6666)
      .filter(_.nonEmpty) //过滤掉空数据
      .map(line => {
        val arr = line.split(" ")
        var uname: String = arr(0).trim
        var timestamp: Long = arr(1).trim.toLong
        (uname: String, timestamp: Long)
      })
      //分配时间戳和水位
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        var maxTimestamp = 0L //迄今为止窗口中最大的时间戳
        val lateness = 10000L //最大允许乱序数据延迟时间
        //为咯查看方便，加入格式
        val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        //获取当前水印----也是在分配水印
        override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - lateness)

        //分配时间戳---从输入数据中提取事件时间
        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          val now_time = element._2 //当前数据时间戳
          //获取最大的时间戳
          maxTimestamp = Math.max(now_time, maxTimestamp)
          //获取当前水印的时间戳
          val nowWaterMark: Long = getCurrentWatermark.getTimestamp
          val threadId = Thread.currentThread().getId
          println(s"线程ID->${threadId} , Event时间→$now_time | ${fmt.format(now_time)}，" +
            s"本窗口迄今为止最大的时间→$maxTimestamp | ${fmt.format(maxTimestamp)}，" +
            s"当前WaterMark→$nowWaterMark | ${fmt.format(nowWaterMark)}")
          now_time //返回当前数据的时间
        }
      }
      )
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .apply(new RichWindowFunction[(String, Long), String, Tuple, TimeWindow] {
        private val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def apply(key: Tuple,
                           window: TimeWindow,
                           input: Iterable[(String, Long)],
                           out: Collector[String]): Unit = {
          //获取整个窗口输入
          val lst = input.iterator.toList.sortBy(_._2)
          val startTime = fmt.format(window.getStart) //该窗口的开始时间
          val endTime = fmt.format(window.getEnd) //取窗口的结束时间

          val result = s"key→${key.getField(0)}，" +
            s"开始EventTime→${fmt.format(lst.head._2)}，" +
            s"结束EventTime→${fmt.format(lst.last._2)}，" +
            s"窗口开始时间→${startTime}，" +
            s"窗口结束时间→${endTime}"
          out.collect(result)
        }
      })

    res.print()

    env.execute("window watermark")
  }
}