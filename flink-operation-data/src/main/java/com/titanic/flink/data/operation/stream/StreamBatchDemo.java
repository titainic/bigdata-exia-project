package com.titanic.flink.data.operation.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamBatchDemo
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "/home/titanic/soft/intellij_workspace/bigdata-exia-project/flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements("Flink batch demo", "batch demo", "demo")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>()
                {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception
                    {
                        for (String word : s.split(" "))
                        {
                            collector.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                }).keyBy(0)
                .sum(1);

        //获取数据到控制台
        System.out.println(env.getExecutionPlan());

        dataStream.print();

        env.execute("StreamBatchDemo");

    }
}
