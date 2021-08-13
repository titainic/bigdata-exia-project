package com.titanic.flink.data.operation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class ReduceonDataSetGroupedbyKeyDemo
{
    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataSet<WC> words = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1));

        DataSet<WC> word = words
                .groupBy("word") //根据word字段对数据集分组
                .reduce(new ReduceFunction<WC>()//在分组数据集上应用ReduceFunction
        {
            @Override
            public WC reduce(WC t0, WC t1) throws Exception
            {
                return new WC(t0.word, t0.count + t1.count);
            }
        });

        word.print();
    }

    public static class WC
    {
        public String word;
        public int count;

        public WC()
        {
        }

        public WC(String word, int count)
        {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString()
        {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}

