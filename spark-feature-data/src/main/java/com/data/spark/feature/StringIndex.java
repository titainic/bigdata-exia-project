package com.data.spark.feature;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
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
 * 将标签字符串 转换为估计器，最频繁的是0
 * 就是新增一列。打上标签
 */
public class StringIndex
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("StringIndex")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create("a"),
                RowFactory.create("b"),
                RowFactory.create("c"),
                RowFactory.create("a"),
                RowFactory.create("a"),
                RowFactory.create("c")
        );

        StructType dfSchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty())

        });

        Dataset<Row> df = spark.createDataFrame(data, dfSchema);

        df.show();

        StringIndexer stringIndexer = new StringIndexer()
                .setInputCol("name")
                .setOutputCol("name_index");

        StringIndexerModel model = stringIndexer.fit(df);
        Dataset<Row> transformDS = model.transform(df);

        transformDS.show();

        spark.stop();

    }
}
