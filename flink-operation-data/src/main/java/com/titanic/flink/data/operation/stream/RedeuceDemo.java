package com.titanic.flink.data.operation.stream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RedeuceDemo
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataStream<Tuple2<String, Integer>> source = env.fromElements(new Tuple2<>("A", 1),
                new Tuple2<>("B", 3),
                new Tuple2<>("C", 6),
                new Tuple2<>("A", 5),
                new Tuple2<>("B", 8));

        DataStream<Tuple2<String, Integer>> reduce = source.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>()
        {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception
            {
                return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1+t1.f1);
            }
        });

        reduce.print();

        env.execute("Reduce Demo");



    }
}
