package com.titanic.flink.data.operation.dataset;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

public class ReduceonDataSetGroupedbyKeySelectorFunction
{
    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-operation-data/target/flink-operation-data-1.0-SNAPSHOT.jar");

        DataSet<WC> words = env.fromElements(new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1));

        DataSet<WC> wordCounts = words.groupBy(new KeySelector<WC, String>()
        {
            @Override
            public String getKey(WC wc) throws Exception
            {
                return wc.word;
            }
        }).reduce(new ReduceFunction<WC>()
        {
            @Override
            public WC reduce(WC wc, WC t1) throws Exception
            {
                return new WC(wc.word, t1.count + wc.count);
            }
        });

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
}
