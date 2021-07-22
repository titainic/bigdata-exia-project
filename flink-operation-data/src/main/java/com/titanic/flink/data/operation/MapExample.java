package com.titanic.flink.data.operation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class MapExample
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataDS = env.readTextFile("flink-operation-data/src/main/resources");
        DataStream<Row> mapDS = dataDS.map(new MapFunction<String, Row>()
        {
            @Override
            public Row map(String s) throws Exception
            {
                String[] array = s.split("  ");

                return Row.of(array[0], array[1]);
            }
        });

        mapDS.print();

        env.execute();
    }
}
