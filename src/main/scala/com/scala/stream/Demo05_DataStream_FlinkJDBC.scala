package scala.com.scala.stream

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row

/**
 * flink自定义source
 *
 * CREATE TABLE `stu1` (
 * `id` int(11) DEFAULT NULL,
 * `name` varchar(32) DEFAULT NULL
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * 自定义source方式获取数据有延迟；；而使用inputformat方式没有延迟
*/
object Demo05_DataStream_FlinkJDBC {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)  //全局设置

    //获取自定义的数据源
    import org.apache.flink.api.scala._

    //定义数据库中的字段类型
    val fieldTypes:Array[TypeInformation[_]] = Array[TypeInformation[_]](
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    //将基础类型转换成行类型
    val rowTypeInfo = new RowTypeInfo(fieldTypes:_*)

    val jdbcInputFormat:JDBCInputFormat = JDBCInputFormat.buildJDBCInputFormat()
   	.setDrivername("com.mysql.jdbc.Driver")
   	.setDBUrl("jdbc:mysql://hadoop01:3306/test")
   	.setQuery("select * from stu1")
    .setUsername("root")
    .setPassword("root")
   	.setRowTypeInfo(rowTypeInfo)
   	.finish()

    //使用createInput方式读取
    val ds: DataStream[Row] = env.createInput(jdbcInputFormat)
    ds.print("flink jdbc->")

    //启动env
    env.execute("flink jdbc")
  }
}