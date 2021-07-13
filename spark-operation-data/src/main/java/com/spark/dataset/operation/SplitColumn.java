package com.spark.dataset.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * 一列拆分多列
 */
public class SplitColumn
{
    public static void main(String[] args)
    {

        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("Dataset2VectorExample")
                .getOrCreate();

        Dataset<Row> data = spark.read()
                .option("header", true)
                .csv("spark-operation-data/src/main/resources/splitcolumn.csv");

        //按照 _ 拆分
        Dataset<Row> splitDS = data.withColumn("split", split(column("f2"), "_"));
        splitDS.show();

        splitDS.printSchema();

        //选取拆分数据组成列
        Dataset<Row> columeSplitDS = splitDS.select(col("f1"),col("split").getItem(0).as("a0"),
                col("split").getItem(1).as("a1"),
                col("split").getItem(2).as("a2"),
                col("split").getItem(3).as("a3"));

        columeSplitDS.show();

        spark.stop();
    }
}
