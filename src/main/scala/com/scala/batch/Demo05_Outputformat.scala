package scala.com.scala.batch

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.com.scala.bean.WordCount

/*
output
 */
object Demo05_Outputformat {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._
    val text: DataSet[String] = env.fromElements("i like flink flink is nice", "aa", "aa")

    val ds: DataSet[WordCount] = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .map(x => WordCount(x._1, x._2))

    ds.output(new MySQLOutputFormat1)

    env.execute("output")
  }
}


class MySQLOutputFormat1 extends OutputFormat[WordCount]{
  override def configure(parameters: Configuration): Unit = {}

  //获取mysql的连接的
  var ps: PreparedStatement = _
  var connection: Connection = _
  var resultSet: ResultSet = _
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop01:3306/test"  //sql连接不用ssl将会报警告
    //val url = "jdbc:mysql://hadoop01:3306/test?useSSL=true"  //用ssl需要配置
    val username = "root"
    val password = "root"
    Class.forName(driver)
    try {
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  //将每接收到的数据写出
  override def writeRecord(record: WordCount): Unit = {
    val sql = "replace into wc(word,count) values(?,?) "
    ps = connection.prepareStatement(sql)
    //为ps赋值
    ps.setString(1,record.word)
    ps.setInt(2,record.count)
    //批次提交
    ps.executeUpdate()
  }

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