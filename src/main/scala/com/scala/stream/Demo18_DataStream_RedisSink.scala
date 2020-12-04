package scala.com.scala.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.com.scala.bean.Yq

/*
* 需求：
 * date provice add possible
 * 2020-10-12 beijing 1 2
 * 2020-10-12 beijing 1 1
 * 2020-10-12 shanghai 1 0
 * 2020-10-12 shanghai 1 1
 *
 * 结果：
 * 2> (2020-5-13_beijing,(1,2))
 * 2> (2020-5-13_beijing,(2,3))
 * 4> (2020-5-13_shanghai,(1,0))
 * 4> (2020-5-13_shanghai,(2,1))
 *
 * 放到redis中
 *
*/
object Demo18_DataStream_RedisSink {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10)

    //输入两个数值，然后根据第一个数值进行累加操作
    import org.apache.flink.api.scala._
    //2020-5-13 beijing 1 2
    val dStream: DataStream[String] = env.socketTextStream("hadoop01", 6666)
    val sumed: DataStream[Yq] = dStream.map(line => {
      val fields: Array[String] = line.split(" ")
      val dt: String = fields(0).toString.trim
      val province: String = fields(1).toString.trim
      val add: Int = fields(2).toInt
      val possible: Int = fields(3).toInt
      //封装返回
      (dt + "_" + province, (add, possible))
    })
      .keyBy(0)
      .reduce((a, b) => (a._1, (a._2._1 + b._2._1, a._2._2 + b._2._2)))
        .map(f=>{
          Yq(f._1.split("_")(0),f._1.split("_")(1),f._2._1,f._2._2)
        })

    sumed.print("adds&possibles->")

    //打入到redis中
    //定义生产相关信息
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setDatabase(1)
      .setHost("192.168.216.111")
      .setPort(6379)
      .setPassword("root")
      .setMaxIdle(10)
      .setMinIdle(3)
      .setTimeout(10000)
      .build()

    //构造Redis的sink
    sumed.addSink(new RedisSink(config,new MyRedisSink))

    env.execute("redis sink")
  }
}

/*
自定义redissink实现
 */
class MyRedisSink extends RedisMapper[Yq] {
  //获取插入redis的命令 ;;如果存储类型为HASH和SORTED_SET需要指定额外的key
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET,null)
  }
  //存储到redis中的key值
  override def getKeyFromData(data: Yq): String = data.dt+"_"+data.province
  //存储到redis中key所对应的value值
  override def getValueFromData(data: Yq): String = data.adds+"_"+data.possibles
}