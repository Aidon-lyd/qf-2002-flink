package scala.com.scala.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.com.scala.bean.Stu

/**
 * flink自定义source
 *
 * CREATE TABLE `stu1` (
 * `id` int(11) DEFAULT NULL,
 * `name` varchar(32) DEFAULT NULL
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * ???思考多个并行度读取一份数据？？？
 * 1、就想并行读取一份数据===》考虑读出来的数据顺序问题
 *
 * 一个并行度读的好处？？所有数据按照顺序读出来
*/
object Demo04_DataStream_MysqlSource {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)  //全局设置

    //获取自定义的数据源
    import org.apache.flink.api.scala._

    val ds: DataStream[Stu] = env.addSource(new MyRichMysqlSource)
    ds.print("mysqlSource->")

    //启动env
    env.execute("mysqlSource")
  }
}

/**
 * 自定义RichParallelSourceFunction
 */
class MyRichMysqlSource extends RichParallelSourceFunction[Stu]{
  //获取mysql的连接的
  var ps: PreparedStatement = _
  var connection: Connection = _
  var resultSet: ResultSet = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"  //sql连接不用ssl将会报警告
    //val url = "jdbc:mysql://hadoop01:3306/test?useSSL=true"  //用ssl需要配置
    val username = "root"
    val password = "root"
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, username, password)
      val sql = "select * from stu1;"
      ps = connection.prepareStatement(sql)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  //读取mysql的数据的
  override def run(ctx: SourceFunction.SourceContext[Stu]): Unit = {
    //获取ps中的数据
    resultSet = ps.executeQuery()
    while (resultSet.next()) {
      var stu = new Stu(
        resultSet.getInt(1),
        resultSet.getString(2).trim)
      ctx.collect(stu)
    }
  }

  override def cancel(): Unit = ???

  //关闭mysql的连接
  override def close(): Unit = {
    if (resultSet != null) {
      resultSet.close()
    }
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}
