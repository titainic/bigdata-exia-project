package com.titanic.flink.data.operation.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataStream<Tuple2<String, Integer>> source1 = env.fromElements(new Tuple2<>("Honda", 15),
                new Tuple2<>("CROWN", 25));

        DataStream<Tuple2<String, Integer>> source2 = env.fromElements( new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40));

        DataStream<Tuple2<String, Integer>> source3 = env.fromElements(new Tuple2<>("Rolls-Royce", 300),
                new Tuple2<>("AMG", 330));

        DataStream<Tuple2<String, Integer>> union = source1.union(source2, source3);
        union.print();

        env.execute("union");
    }
}
