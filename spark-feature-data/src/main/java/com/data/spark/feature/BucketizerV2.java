package com.data.spark.feature;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.BarTrace;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class BucketizerV2
{

    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder()
                .master("spark://titanic:7077")
                .appName("BucketizerV2")
                .getOrCreate();

        spark.sparkContext().addJar("spark-feature-data/target/spark-feature-data-1.0-SNAPSHOT.jar");

        Dataset<Row> data = spark
                .read()
//                .option("header", "true")
                .format("com.databricks.spark.csv")
                .load("spark-feature-data/src/main/resources/catering_fish_congee.csv");

        Dataset<Row> reNameDS = data.select(col("_c1").cast(DataTypes.IntegerType).as("sale"), col("_c0").as("date"));

        //定义数据区间数组,一下数组区间范围[0-500,500-1000,1000-1500,1500-2000,2000-2500,2500-3000,3000-3500,3500-4000]
        double[] splits = {0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000};

        Bucketizer bucketizer = new Bucketizer()
                .setInputCol("sale")
                .setOutputCol("sale_layer")
                .setSplits(splits);


        Dataset<Row> bucketedData = bucketizer.transform(reNameDS);

        System.out.println("数据切分成8片:" + (bucketizer.getSplits().length - 1));
        bucketedData.orderBy("sale").show(100);

        Dataset<Row> xyDS = bucketedData.groupBy(col("sale_layer")).agg(sum(col("sale")).as("sum")).orderBy("sale_layer");


        List<String> xList = xyDS.select("sale_layer").map(new MapFunction<Row, String>()
        {

            public String call(Row row) throws Exception
            {
                return row.get(0) + "";
            }
        }, Encoders.STRING()).collectAsList();

        List<Double> yList = xyDS.select("sum").map(new MapFunction<Row, Double>()
        {

            public Double call(Row row) throws Exception
            {
                return Double.valueOf(row.getLong(0));
            }
        }, Encoders.DOUBLE()).collectAsList();

        spark.stop();

        plot(xList, yList);

    }

    /**
     * 可视化
     * @param xList
     * @param yList
     */
    public static void plot(List<String> xList, List<Double> yList)
    {
        String[] x = new String[xList.size()];
        xList.toArray(x);

        double[] y = new double[yList.size()];
        for (int i = 0; i < yList.size(); i++)
        {
            y[i] = yList.get(i).doubleValue();
        }

        Layout layout = Layout.builder().title("数据区间分箱操作").build();
        BarTrace trace = BarTrace.builder(x, y).build();
        Plot.show(new Figure(layout, trace));
    }
}
