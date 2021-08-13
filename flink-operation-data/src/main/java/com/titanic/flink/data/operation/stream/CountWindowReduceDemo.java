package com.titanic.flink.data.operation.stream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CountWindowReduceDemo
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "/home/titanic/soft/intellij_workspace/bigdata-exia-project/flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataStream<Tuple2<String, Long>> input = env.fromElements(
                new Tuple2("BMW", 2L),
                new Tuple2("BMW", 2L),
                new Tuple2("Tesla", 3L),
                new Tuple2("Tesla", 4L));

        DataStream<Tuple2<String, Long>> output = input
                .keyBy(0)
                .countWindow(2)
                .reduce(new ReduceFunction<Tuple2<String, Long>>()
                {
                    @Override
                    public Tuple2 reduce(Tuple2<String, Long> t0, Tuple2<String, Long> t1) throws Exception
                    {
                        return new Tuple2<String, Long>(t0.f0,t1.f1+t1.f1);
                    }
                });

        output.print();

        env.execute("CountWindowReduceDemo");
    }
}
