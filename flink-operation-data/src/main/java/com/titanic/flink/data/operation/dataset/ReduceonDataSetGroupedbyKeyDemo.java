package com.titanic.flink.data.operation.dataset;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class ReduceonDataSetGroupedbyKeyDemo
{
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<WC> words = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1));
        DataSet<WC> wordCounts = words
                //根据word字段对数据集分组
                .groupBy("word")
                //在分组数据集上应用ReduceFunction
                .reduce(new WordCounter());
        wordCounts.print();
    }


    public static class WC {
        public String word;
        public int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
    // 求和count字段的ReduceFunction
    public static class WordCounter implements ReduceFunction<WC>
    {
        @Override
        public WC reduce(WC in1, WC in2) {
            return new WC(in1.word, in1.count + in2.count);
        }
    }
}
