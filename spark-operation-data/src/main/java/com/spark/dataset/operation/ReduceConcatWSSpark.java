package com.spark.dataset.operation;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.sql.AnalysisException;
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
 * 类似Ruduce操作
 * concat_ws 多列相同的key，组合后面的value，以'_' 分隔
 */
public class ReduceConcatWSSpark
{
    public static void main(String[] args) throws AnalysisException
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("ReduceConcatWSSpark")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create("a", 1.0),
                RowFactory.create("a", 1.0),
                RowFactory.create("a", 2.0),
                RowFactory.create("b", 3.0),
                RowFactory.create("b", 4.0),
                RowFactory.create("c", 5.0),
                RowFactory.create("d", 6.0)
        );

        StructType dfSchema = new StructType(new StructField[]{
                new StructField("Index1", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Index2", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, dfSchema);

        df.createTempView("df");
                                                                            //collect_set去重
        Dataset<Row> ds1 = spark.sql(" select  Index1, concat_ws('_',collect_list(Index2)) as mids from df group by Index1");
        ds1.show();

        spark.stop();
    }
}

