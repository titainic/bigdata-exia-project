package com.titanic.flink.data.operation.dataset;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapPartitionDemo
{
    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataSet<String> textLines = env.fromElements("BMW", "Tesla", "Rolls-Royce");

        DataSet<Long> mapPartition = textLines.mapPartition(new MapPartitionFunction<String, Long>()
        {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Long> collector) throws Exception
            {
                long i = 0;
                for (String value : iterable)
                {
                    i++;
                }
                collector.collect(i);
            }
        });

        mapPartition.print();
    }
}
