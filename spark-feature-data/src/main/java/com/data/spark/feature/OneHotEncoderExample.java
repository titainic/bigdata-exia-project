package com.data.spark.feature;

import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
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
 * spark One-Hot-Encoder 操作
 */
public class OneHotEncoderExample
{

  public static void main(String[] args)
  {
      SparkSession spark = SparkSession
              .builder()
              .master("spark://titanic:7077")
              .appName("OneHotEncoderExample")
              .getOrCreate();


      List<Row> data = Arrays.asList(
              RowFactory.create("周一", 1.0,"a"),
              RowFactory.create("周二", 0.0,"b"),
              RowFactory.create("周三", 1.0,"c"),
              RowFactory.create("周四", 2.0,"a"),
              RowFactory.create("周五", 1.0,"b"),
              RowFactory.create("周六", 0.0,"c"),
              RowFactory.create("周日", 1.0,"a")
      );

      StructType schema = new StructType(new StructField[]{
              new StructField("categoryIndex1", DataTypes.StringType, false, Metadata.empty()),
              new StructField("categoryIndex2", DataTypes.DoubleType, false, Metadata.empty()),
              new StructField("categoryIndex3", DataTypes.StringType, false, Metadata.empty())
      });

      Dataset<Row> df = spark.createDataFrame(data, schema);

      Dataset<Row>  sampleIndexedDf = new StringIndexer().setInputCol("categoryIndex1")
                                                         .setOutputCol("categoryVec1")
                                                         .fit(df)
                                                         .transform(df);

      OneHotEncoder encoder = new OneHotEncoder()
              .setInputCol("categoryVec1")
              .setOutputCol("categoryVec_vec");
//              .setOutputCols(new String[] {"categoryIndex3", "categoryVec3"});

      OneHotEncoderModel model = encoder.fit(sampleIndexedDf);
      Dataset<Row> encoded = model.transform(sampleIndexedDf);

      encoded.show();
      encoded.printSchema();

      spark.stop();
  }


}
