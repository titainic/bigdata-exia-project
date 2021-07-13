package com.data.spark.feature;

import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.split;

/**
 * 归一化,标准化
 * 将特征值缩放到一个指定的负数和正数之间
 * https://blog.csdn.net/xuejianbest/article/details/85779029 参考
 */
public class Normalize
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
        data.show();

        //拆分一列为数组
        Dataset<Row> splitDS = data.withColumn("split", split(column("value"), ",")).select("split");
        splitDS.show(false);

        //把数组，单独变成一列
        Dataset<Row> fieldDS = splitDS.select(col("split").getItem(0).as("id").cast(DataTypes.DoubleType),
                col("split").getItem(1).as("a1").cast(DataTypes.DoubleType),
                col("split").getItem(2).as("a2").cast(DataTypes.DoubleType),
                col("split").getItem(3).as("a3").cast(DataTypes.DoubleType),
                col("split").getItem(4).as("a4").cast(DataTypes.DoubleType),
                col("split").getItem(5).as("a5").cast(DataTypes.DoubleType),
                col("split").getItem(6).as("a6").cast(DataTypes.DoubleType),
                col("split").getItem(7).as("a7").cast(DataTypes.DoubleType),
                col("split").getItem(8).as("a8").cast(DataTypes.DoubleType),
                col("split").getItem(9).as("a9").cast(DataTypes.DoubleType),
                col("split").getItem(10).as("a10").cast(DataTypes.DoubleType),
                col("split").getItem(11).as("a11").cast(DataTypes.DoubleType),
                col("split").getItem(12).as("a12").cast(DataTypes.DoubleType),
                col("split").getItem(13).as("a13").cast(DataTypes.DoubleType)
        );

        fieldDS.show(false);
        fieldDS.printSchema();

        //把列转换成向量
        String[] col = {"a1", "a2","a3","a4","a5","a6","a7","a8","a9","a10","a11","a12","a13"};
        VectorAssembler assembler = new VectorAssembler();

        //选择要vector的列
        assembler.setInputCols(col).setOutputCol("feature");
        Dataset<Row> vectorDS = assembler.transform(fieldDS);

        //选取要使用的列
        Dataset<Row> feautreDS = vectorDS.select("id", "feature");
        feautreDS.show(false);

        //归一化
        MinMaxScaler minMaxScaler = new MinMaxScaler()
                .setInputCol("feature")
                .setOutputCol("scaled")
                .setMax(1)    //最大值
                .setMin(-1);  //最小值

        minMaxScaler.fit(feautreDS).transform(feautreDS).select("scaled").show(false);


        //标准化
        StandardScaler scaler = new StandardScaler()
                .setInputCol("feature")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);

        scaler.fit(feautreDS).transform(feautreDS).select("scaledFeatures").show(false);
        spark.stop();

    }
}
