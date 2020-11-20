package com.spark.dataset.operation;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * 普通数据库加载的数据Dataset<Row>（普通表结构）转换成 label　features＜向量vector＞格式，进行机器学习计算
 */
public class Dataset2VectorExample
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("Dataset2VectorExample")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, 1.0,20.0,1.0,30.0),
                RowFactory.create(1.0, 0.0,20.0,1.0,30.0),
                RowFactory.create(2.0, 1.0,20.0,1.0,30.0),
                RowFactory.create(0.0, 2.0,20.0,1.0,30.0),
                RowFactory.create(0.0, 1.0,20.0,1.0,30.0),
                RowFactory.create(2.0, 0.0,20.0,1.0,30.0)
        );

        StructType dfSchema = new StructType(new StructField[]{
                new StructField("Index1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index2", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index3", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index4", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index5", DataTypes.DoubleType, false, Metadata.empty()),
        });

        Dataset<Row> df = spark.createDataFrame(data, dfSchema);

        df.show();

        String[] col = {"Index1", "Index2","Index3","Index4","Index5"};
        VectorAssembler assembler = new VectorAssembler();

        //选择要vector的列
        assembler.setInputCols(col).setOutputCol("features");
        Dataset<Row> vectorDS = assembler.transform(df);

        vectorDS.show();

        spark.stop();
    }
}
