package com.titanic.flink.data.operation.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Aggregate算子只能用于元组数据集(Tuple),并且仅支持用于分组的字段位置建
 */
public class AggregateGroupedTupleDemo
{
    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3(1, "a", 1.0),
                new Tuple3(2, "b", 2.0),
                new Tuple3(4, "b", 4.0),
                new Tuple3(3, "c", 3.0));
        AggregateOperator<Tuple3<Integer, String, Double>> output1 = input
                .groupBy(1)//在字段2上进行分组
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.MIN, 2);//产生字段0的总和和字段2的最小值原始数据集

        AggregateOperator<Tuple3<Integer, String, Double>> output2 = input
                .groupBy(1) //在字段2上进行分组
                .aggregate(Aggregations.SUM, 0)
                .aggregate(Aggregations.MIN, 2);//在聚合上应用聚合,将在计算按字段1分组的字段0的总和后产生字段2的最小值

        output1.print();

        System.out.println("-------------------");

        output2.print();



    }
}
