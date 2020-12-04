package scala.com.scala.stream

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/*
 * 重启策略
 *
*/
object Demo28_DataStream_Restart {
  def main(args: Array[String]): Unit = {

    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    try{
    //设置statebackend//设置statebackend
    //env.setStateBackend(new MemoryStateBackend())    //状态数据存储于内存
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/checkpoint/flink2002")) //状态存储于hdfs中
    //需要引入RocksDB的依赖
    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:9000/flink_rocksDB",true))

    //获取checkpoint配置
    val conf: CheckpointConfig = env.getCheckpointConfig

    // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    /*
    .RETAIN_ON_CANCELLATION： 取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
    .DELETE_ON_CANCELLATION： 取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
    */
    conf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的周期, 每隔2000 ms进行启动一个检查点
    conf.setCheckpointInterval(2000)
    // 设置模式为exactly-once
    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少1000 ms的间隔【checkpoint最小间隔】
    conf.setMinPauseBetweenCheckpoints(1000)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    conf.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    conf.setMaxConcurrentCheckpoints(1)

      //配置重启策略
      //没有重启策略
      //env.setRestartStrategy(RestartStrategies.noRestart())

      //定期间隔重启
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(10)))

      //基于失败率的重启
      //env.setRestartStrategy(RestartStrategies.failureRateRestart(1,Time.seconds(10),Time.seconds(10)))

      //fallback重启策略
      //env.setRestartStrategy(RestartStrategies.fallBackRestart())

    //job
    import org.apache.flink.api.scala._
    env.socketTextStream("hadoop01",6666)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("checkpoint")
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}

