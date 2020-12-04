package scala.com.scala.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/*
blink的sql
kafka2mysql
 */
object Demo09_kafka2sql_blink_2004 {
  def main(args: Array[String]): Unit = {

    //设置执行环境为blink解析器
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    //获取Blink的table执行环境
    val tenv: TableEnvironment = TableEnvironment.create(settings)

    //Aflink执行环境 --- aflink的执行环境不能使用使用如下的操作形式
    /*val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv: TableEnvironment = StreamTableEnvironment.create(env)*/

    //执行create table语句
    tenv.sqlUpdate(
      s"""
         |CREATE TABLE user_log (
         |    user_id VARCHAR,
         |    item_id VARCHAR,
         |    category_id VARCHAR,
         |    action VARCHAR,
         |    ts TIMESTAMP
         |) WITH (
         |    'connector.type' = 'kafka', -- 使用 kafka connector
         |    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本
         |    'connector.topic' = 'test',  -- kafka topic
         |    'connector.properties.0.key' = 'zookeeper.connect',  -- 连接信息
         |    'connector.properties.0.value' = 'hadoop01:2181',
         |    'connector.properties.1.key' = 'bootstrap.servers',
         |    'connector.properties.1.value' = 'hadoop01:9092',
         |    'update-mode' = 'append',  --更新模式，，追加形式
         |    'connector.startup-mode' = 'latest-offset',  -- earliest-offset:从起始 offset 开始读取
         |    'format.type' = 'json',  -- 数据源格式为 json
         |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
         |)
         |""".stripMargin)

    //使用sql执行---transformation
    //执行查询语句 replace 和 ON DUPLICATE KEY UPDATE都不支持,,因为这两是mysql的语法,非标准通用语法
    //INSERT overwrite mysql不支持该语法
    tenv.sqlUpdate(
      s"""
         |replace into pvuv_sink
         |SELECT
         |  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
         |  COUNT(*) AS pv,
         |  COUNT(DISTINCT user_id) AS uv
         |FROM user_log
         |GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')
         |""".stripMargin)

    //执行创建表语句
    tenv.sqlUpdate(
      s"""
         |CREATE TABLE pvuv_sink (
         |    dt VARCHAR,
         |    pv BIGINT,
         |    uv BIGINT
         |) WITH (
         |    'connector.type' = 'jdbc', -- 使用 jdbc connector
         |    'connector.url' = 'jdbc:mysql://hadoop01:3306/yq_report', -- jdbc url
         |    'connector.table' = 'pvuv_sink', -- 表名
         |    'connector.username' = 'root', -- 用户名
         |    'connector.password' = 'root', -- 密码
         |    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
         |)
         |""".stripMargin)

    tenv.execute("blink sql")
  }
}
