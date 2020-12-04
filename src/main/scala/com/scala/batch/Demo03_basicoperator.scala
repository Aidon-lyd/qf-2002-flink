package scala.com.scala.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment

/*
常见算子
 */
object Demo03_basicoperator {
  def main(args: Array[String]): Unit = {
    //1、获取批次执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    import org.apache.flink.api.scala._
    val text: DataSet[String] = env.fromElements("i like flink flink flink is nice nice")

    //map:一个数据生成一个新的数据
    text.map(x=>(x,1)).print()

    //flatmap:一个数据生成多个新的数据
    text.flatMap(x=>x.split(" ")).print()

    //mappartition:函数处理包含一个分区所有数据的“迭代器”，
    // 可以生成任意数量的结果值。每个分区中的元素数量取决于并行度和先前的算子操作。
    text.mapPartition(x=>x map((_,1))).print()

    //filter:执行布尔函数，只保存函数返回 true 的数据。
    text.filter(x=>x.length>20).print()

    //Distinct:对数据集中的元素除重并返回新的数据集。
    text.distinct().print()
    text.flatMap(x=>x.split(" ")).map((_,1)).distinct(0).print("dis2")
    //自定义类型，，根据字段去重,,如下有报错
     case class Word(word : String)
     text.flatMap(x=>x.split(" ")).map(x=>Word(x)).distinct("word").print("dis3")

    //Reduce:作用于整个 DataSet，合并该数据集的元素。
    val data = env.fromElements(11,22,33)
    data.reduce(_ + _).print()


    //Aggregate:聚合，对一组数据求聚合值，聚合可以应用于完整数据集或分组数据集。
    // 聚合转换只能应用于元组（Tuple）数据集，并且仅支持字段位置键进行分组。
    //常见聚合函数：min、max、sum
    val data1: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "zs", 16d), (1, "ls", 20d), (2, "goudan", 23d), (3, "mazi", 30d)
    )
    data1.aggregate(Aggregations.SUM, 0).print()
    data1.aggregate(Aggregations.SUM, 0).aggregate(Aggregations.MIN, 2).print()
    // 输出 (7,c,16.0)
    // 简化语法
    data1.sum(0).min(2).print()

    //MinBy / MaxBy:取元组数据集中指定一个或多个字段的值最小（最大）的元组，
    // 可以应用于完整数据集或分组数据集。用于比较的字段必须可比较的。
    // 如果多个元组具有最小（最大）字段值，则返回这些元组的任意元组。
    // 比较元组的第一个字段
    data1.minBy(0).print()
    // 输出 (1,b,20.0)
    // 比较元组的第一、三个字段
    data1.minBy(0,2).print()

    //groupBy:用来将数据分组
    // 根据元组的第一和第二个字段分组
    data1.groupBy(0, 1)
    data1.groupBy(0).sortGroup(1, Order.ASCENDING) //分组并排序
    data1.groupBy(1).sum(0).print("==")
    data1.groupBy(1).sum(0).max(2).print()  //.groupBy(0).max(2)

    //join:将两个 DataSet 连接生成一个新的 DataSet。默认是inner join
    val d1: DataSet[(String, Int)] = env.fromElements(("a", 11), ("b", 2), ("a", 33))
    val d2: DataSet[(String, Int)] = env.fromElements(("a", 12), ("b", 22), ("c", 56))
    d1.join(d2).where(0).equalTo(0).print()

    //Union:构建两个数据集的并集。
    d1.union(d2).print()

    //Cross:构建两个输入数据集的笛卡尔积。 非keyvalue类型需要自定义
    d1.cross(d2).print()

    //Rebalance:均匀地重新负载数据集的并行分区以消除数据偏差。后面只可以接类似 map 的算子操作。
    text.rebalance().map(x=>(x,1)).print()

    //hash-Partition :根据给定的 key 对数据集做 hash 分区。可以是 position keys，expression keys 或者 key selector functions。
    text.rebalance().map(x=>(x,1)).partitionByHash(0).print()

    //类似分区还有Range-Partition、Sort Partition和自定义分区---自行查看

    //First-n: 返回数据集的前n个元素。可以应用于任意数据集。类似topN
    //i like flink flink is nice
    /*
    i 1
    like 1
    flink 2
    is 1
    nice 1
     */
    text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).groupBy(0).sortGroup(1, Order.DESCENDING).first(3).print()

    env.execute("batch transformation")
  }
}
