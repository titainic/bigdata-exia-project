package com.data.spark.feature;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 拆分数据集80%，20%比例
 */
public class SplitDsTestAndTrain
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("Normalize")
                .getOrCreate();

        //加载数据
        Dataset<Row> data = spark.read().text("spark-feature-data/src/main/resources/wine.data");

        System.out.println(data.count());

        Dataset<Row>[] splitDS = data.randomSplit(new double[]{0.8, 0.2});

        Dataset<Row> trainDS = splitDS[0];
        Dataset<Row> testDS = splitDS[1];

        System.out.println(trainDS.count());
        System.out.println(testDS.count());


        spark.stop();
    }


}
