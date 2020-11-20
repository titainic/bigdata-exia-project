package com.spark.statistics.operation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * spark基础统计,均值，总数，标准差，最小，最大
 */
public class DescribeExampleSpark
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("DescribeExampleSpark")
                .getOrCreate();

        Dataset<Row> irisDS = spark.read()
                                   .option("header",true)
                                   .csv("spark-statistics-data/src/main/resources/iris.csv");

        irisDS.describe("SepalLength").show();
        spark.stop();
    }
}
