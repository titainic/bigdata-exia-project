package com.titanic.flink.data.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * Flink自定义数据源
 */
public class DataSourceFlinkExample
{
    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> data = env.addSource(new SourceFunction<Row>()
        {
            private boolean running = true;

            @Override
            public void run(SourceContext<Row> sourceContext) throws Exception
            {
                Random random = new Random();

                while (running)
                {
                    sourceContext.collect(Row.of(random.nextGaussian(),random.nextInt()));
                }

                // 控制输出频率
                Thread.sleep(10000);
            }

            @Override
            public void cancel()
            {
                running = false;
            }
        });

        data.print();

        env.execute();

    }
}
