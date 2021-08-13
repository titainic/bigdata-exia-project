package com.titanic.flink.data.operation.stream;

import com.titanic.flink.data.operation.source.MySource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowAndWatermarkDemo
{
    public static void main(String[] args)
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<String> input = env.addSource(new MySource());

        DataStream<Tuple2<String,Long>> inputMap = input.map(new MapFunction<String, Tuple2<String, Long>>()
        {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception
            {
                String[] arr = s.split(",");
                return new Tuple2<String, Long>(arr[0], Long.parseLong(args[1]));
            }
        });

//        inputMap.assignTimestampsAndWatermarks()


    }
}
