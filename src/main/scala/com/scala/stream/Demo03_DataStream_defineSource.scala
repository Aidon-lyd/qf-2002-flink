package scala.com.scala.stream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/**
 * flink自定义source
 * 自定义source分为3类：
 * 1、SourceFunction: 实现该接口，该类型的source不能设置并行度；同样没有open()和close()方法
 *  典型的就是SocketTextStreamFunction就是实现SourceFunction。
 * 2、ParallelSourceFunction : 实现该接口，能设置并行度；但没有open()和close()方法
 * 3、RichParallelSourceFunction ： 实现该接口，能设置并行度；同样有open()和close()方法
 */
object Demo03_DataStream_defineSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取自定义的数据源
    import org.apache.flink.api.scala._

    //1、自定义sourcefunction
    /*val ds: DataStream[String] = env.addSource(new SourceFunction[String] {
      //负责将生成数据发送到下游
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random: Random = new Random()
        // 循环可以不停的读取静态数据
        while (true) {
          val nextInt = random.nextInt(30)
          ctx.collect("random : " + nextInt)
          Thread.sleep(500);
        }
      }
      //负责什么时候结束run方法
      override def cancel(): Unit = ???
    })  //.setParallelism(2)  //不能在source上设置并行度
    ds.print("sourcefunction=>").setParallelism(2)*/

    //2、自定义ParallelSourceFunction
    /*val ds: DataStream[String] = env.addSource(new MyParallelSourceFunction).setParallelism(2)
    ds.print("ParallelSourceFunction->")*/

    val ds: DataStream[String] = env.addSource(new MyRichParallelSourceFunction).setParallelism(2)
    ds.print("RichParallelSourceFunction->")

    //启动env
    env.execute("collection source")
  }
}

//自定义ParallelSourceFunction
class MyParallelSourceFunction extends ParallelSourceFunction[String]{
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    // 循环可以不停的读取静态数据
    while (true) {
      val nextInt = random.nextInt(30)
      ctx.collect("random : " + nextInt)
      Thread.sleep(500);
    }
  }

  override def cancel(): Unit = ???


}


/**
 * 自定义RichParallelSourceFunction
 */
class MyRichParallelSourceFunction extends RichParallelSourceFunction[String]{

  var index:Int = _
  override def open(parameters: Configuration): Unit = {
    index = 50
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random: Random = new Random()
    // 循环可以不停的读取静态数据
    while (true) {
      val nextInt = random.nextInt(index)
      ctx.collect("random : " + nextInt)
      Thread.sleep(500);
    }
  }

  override def cancel(): Unit = ???

  override def close(): Unit = super.close()
}
