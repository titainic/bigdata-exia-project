package com.titanic.flink.data.operation.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 累加器，在调试运行期间，累加器能观察任务的数据变化，在作业结束自后获取累加器结果
 */
public class AccumulatorDemo
{

    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("titanic", 18081, "flink-example/target/flink-example-1.0-SNAPSHOT.jar");

        DataStream<String> input = env.fromElements("BMW", "Tesla", "Rolls-Royce");

        DataStream<String> result = input.map(new RichMapFunction<String, String>()
        {
            //创建累加器
            IntCounter intCounter = new IntCounter();

            //注册累加器
            @Override
            public void open(Configuration parameters) throws Exception
            {
                super.open(parameters);
                getRuntimeContext().addAccumulator("myAccumulatorName", intCounter);
            }

            //使用累加器
            public String map(String s) throws Exception
            {
                intCounter.add(1);
                return s;
            }
        }).setParallelism(1);//如果并行度不是1，多个并行度。普通累加器求和结果不准确

        result.print();

        JobExecutionResult myJob = env.execute("myJob");

        //获取累加器的计算结果
        int myAccumulatorName = myJob.getAccumulatorResult("myAccumulatorName");

        System.out.println(myAccumulatorName);
    }
}
