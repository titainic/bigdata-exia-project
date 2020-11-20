package com.spark.dataset.operation;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * spark DataSet<Row> Map操作和StructType,RowEncoder 运用
 */
public class MapExampleSpark
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("MapExampleSpark")
                .getOrCreate();

        /**
         * 使用DataSet<Row> map操作，必须要字步骤，添加jar
         */
        spark.sparkContext().addJar("spark-operation-data/target/spark-operation-data-1.0-SNAPSHOT.jar");

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, 1.0),
                RowFactory.create(1.0, 0.0),
                RowFactory.create(2.0, 1.0),
                RowFactory.create(0.0, 2.0),
                RowFactory.create(0.0, 1.0),
                RowFactory.create(2.0, 0.0)
        );

        StructType dfSchema = new StructType(new StructField[]{
                new StructField("Index1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index2", DataTypes.DoubleType, false, Metadata.empty())
        });

        StructType dataSchema = new StructType(new StructField[]{
                new StructField("Index1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index2", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Index3", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, dfSchema);

        df.show();

        Dataset<Row> dataDS = df.map(new MapFunction<Row, Row>()
        {
            public Row call(Row row) throws Exception
            {
                return RowFactory.create(Double.valueOf(row.getAs("Index1")+""),Double.valueOf(row.getAs("Index2")+""), "c");
            }
        }, RowEncoder.apply(dataSchema));

        dataDS.show();

        spark.stop();
    }
}
