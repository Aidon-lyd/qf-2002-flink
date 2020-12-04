package com.qianfeng.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo01_WordCount_java {
    public static void main(String[] args) throws Exception {
        //1、获取flink的上下文执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2、初始化数据源 ---> source
        DataStreamSource<String> ds = env.socketTextStream("hadoop01", 6666);
        //3、对数据源ds进行转换 ---> transformation
       /* SingleOutputStreamOperator<WordWithCount> flatMapDS = ds.flatMap(new FlatMapFunction<String, WordWithCount>() {
            //实现切分--
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                //先对value进行切分
                String[] words = value.split(" ");
                //循环输出
                WordWithCount wordWithCount = new WordWithCount();
                for (String word : words) {
                    //搜集返回值
                    wordWithCount.word = word;
                    wordWithCount.count = 1;
                    out.collect(wordWithCount);
                }
            }
        });

         //4、进行分组累加
        SingleOutputStreamOperator<WordWithCount> sumed = flatMapDS.keyBy("word")
                .sum("count");


        //5、进行sink
        sumed.print("wc->");*/

        /*SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                //先对value进行切分
                String[] words = value.split(" ");
                //循环输出
                for (String word : words) {
                    //搜集返回值
                    out.collect(new Tuple2<String, Long>(word, 1l));
                }
            }
        });*/

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = ds.flatMap(new MyFlatMapFunction());

        SingleOutputStreamOperator<Tuple2<String, Long>> sumed = flatMapDS.keyBy(0).sum(1);
        sumed.print("wc->");

        //6、触发执行
        env.execute("java wc");
    }

    //自定义封装数据对象的内部类
    public static class WordWithCount {
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" + "word='" + word +", count=" + count +'}';
        }
    }


    //自定义flatmap函数
    public static class MyFlatMapFunction implements FlatMapFunction<String,Tuple2<String,Long>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            //先对value进行切分
            String[] words = value.split(" ");
            //循环输出
            for (String word : words) {
                //搜集返回值
                out.collect(new Tuple2<String, Long>(word, 1l));
            }
        }
    }
}


