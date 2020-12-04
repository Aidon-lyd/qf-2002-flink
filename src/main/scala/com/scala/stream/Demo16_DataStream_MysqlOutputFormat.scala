package scala.com.scala.stream

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.api.common.io.{InputFormat, OutputFormat}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

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
 * 放到MySQL中:
 * mysql表结构：
 CREATE TABLE `yq_2002` (
 `dt` varchar(255) NOT NULL,
 `province` varchar(255) NOT NULL,
 `adds` int(10) DEFAULT '0',
 `possibles` int(10) DEFAULT '0',
 PRIMARY KEY (`dt`,`province`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
*/
object Demo16_DataStream_MysqlOutputFormat {
  def main(args: Array[String]): Unit = {
    //1、获取流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

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

    //打入到mysql中
    sumed.writeUsingOutputFormat(new MySQLOutputFormat)

    env.execute("mysql outputformat")
  }
}

/*
自定义outputformat
 */
class MySQLOutputFormat extends OutputFormat[Yq]{
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
  override def writeRecord(record: Yq): Unit = {
    val sql = "replace into yq_2002(dt,province,adds,possibles) values(?,?,?,?) "
    ps = connection.prepareStatement(sql)
    //为ps赋值
    ps.setString(1,record.dt)
    ps.setString(2,record.province)
    ps.setInt(3,record.adds)
    ps.setInt(4,record.possibles)
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