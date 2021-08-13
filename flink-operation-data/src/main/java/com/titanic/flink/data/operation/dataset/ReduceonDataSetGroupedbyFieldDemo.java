package com.titanic.flink.data.operation.dataset;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceonDataSetGroupedbyFieldDemo
{
    public static void main(String[] args) throws Exception
    {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<String, Integer, Double>> tuples = env.fromElements(
                Tuple3.of("BMW", 30, 2.0),
                Tuple3.of("Tesla", 30, 2.0),
                Tuple3.of("Tesla", 30, 2.0),
                Tuple3.of("Rolls-Royce", 300, 4.0)
        );
        DataSet<Tuple3<String, Integer, Double>> reducedTuples = tuples
                // group DataSet on first and second field of Tuple
                .groupBy(0, 1)
                // apply ReduceFunction on grouped DataSet
                .reduce(new ReduceFunction<Tuple3<String, Integer, Double>>()
                {
                    @Override
                    public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> value1, Tuple3<String, Integer, Double> value2) throws Exception {
                        return new Tuple3<>(value1.f0,value1.f1+value2.f1,value1.f2);
                    }
                });
        reducedTuples.print();

    }
}
