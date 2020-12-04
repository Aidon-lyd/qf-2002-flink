package com.qianfeng.batch;

import com.qianfeng.stream.Demo01_WordCount_java;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

//java版本的批次的api统计
public class Demo01_WordCount_batch_java {
    public static void main(String[] args) throws Exception {
        //获取批次执行上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("i like flink","flink is nice","flink nice")
                .flatMap(new Demo01_WordCount_java.MyFlatMapFunction())
               // .filter(new FilterFunction<Tuple2<String, Long>>() {}
                .groupBy(0)
                .sum(1)
                .print();
    }
}
