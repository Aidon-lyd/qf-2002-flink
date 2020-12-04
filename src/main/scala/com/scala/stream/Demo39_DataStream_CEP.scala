package scala.com.scala.stream

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.com.scala.bean.{Event, SubEvent}

/*
 * flink的cep
 * cep案例：
1、过滤事件流规则为：start开始--->任意--->middle --->任意--->最后end
2、过滤事件流规则为：start开始--->middle --->任意--->最后end
3、过滤事件流规则为：start开始--->任意--->middle(3次) --->任意--->最后end
 *
*/
object Demo39_DataStream_CEP {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val ds: DataStream[Event] = env.fromElements[Event](
      new Event(1, "start", 1.0), //理解为→ 登陆旅游网站
      new Event(2, "middle", 2.0), //理解为→ 浏览旅游网站上的旅游景点的信息
      new Event(3, "foobar", 3.0),
      new SubEvent(4, "foo", 4.0, 1.0),
      new Event(5, "middle", 5.0),
      new SubEvent(6, "middle", 6.0, 2.0),
      new SubEvent(7, "bar", 3.0, 3.0),
      new Event(42, "42", 42.0),
      new Event(8, "end", 1.0) //理解为→ 退出旅游网站
    )

    //定义parttern
    //宽松近邻 start:1 middle:2.0 end:1.0 ;;; start:1 middle:5.0 end:1.0 ;;; start:1 middle:6.0 end:1.0
    /*val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))  //匹配数据流中的name是否为start
      .followedByAny("middle")
      .where(_.getName.equals("middle")) //浏览
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))

    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))  //匹配数据流中的name是否为start
      .next("middle")
      .where(_.getName.equals("middle")) //浏览
      .where(_.getPrice>5.0)
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))

    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))  //匹配数据流中的name是否为start
      .followedByAny("middle")
      .where(_.getName.equals("middle")) //浏览
      .times(5)
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))*/

    val patternRule = Pattern.begin[Event]("start")
      .where(_.getName.equals("start"))  //匹配数据流中的name是否为start
      .followedByAny("middle")
      .where(_.getName.equals("middle")) //浏览
      .followedByAny("end") //退出
      .where(_.getName.equals("end"))
      .within(Time.seconds(10))

    //将规则应用于输入的数据流
    val ps: PatternStream[Event] = CEP.pattern(ds, patternRule)

    //从partternStream中取出结果数据
    //从匹配模式流中取出数据，并予以显示
    ps.flatSelect[String]((ele: scala.collection.Map[String, Iterable[Event]], out: Collector[String]) => {
      //步骤：
      //准备一个容器，如：StringBuilder，用于存放结果
      val builder = new StringBuilder

      //从匹配模式流中取出对应的元素,并追加到容器中 --- get(name)需要和followedBy(name)一致
      val startEvent = ele.get("start").get.toList.head.toString
      val middleEvent = ele.get("middle").get.toList.head.toString
      val endEvent = ele.get("end").get.toList.head.toString

      builder.append(startEvent).append("\t|\t")
        .append(middleEvent).append("\t|\t")
        .append(endEvent)

      //将结果通过Collector发送到新的DataStream中存储起来
      out.collect(builder.toString)
    }).print("CEP 运行结果 ： ")

    env.execute("cep")
  }
}