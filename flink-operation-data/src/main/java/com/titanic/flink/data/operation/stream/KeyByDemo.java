package com.titanic.flink.data.operation.stream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                new Tuple2<>("Honda", 15),
                new Tuple2<>("Honda", 10),
                new Tuple2<>("CROWN", 25),
                new Tuple2<>("BMW", 35));

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = input.keyBy(0);

        keyBy.print();

        env.execute();
    }
}
