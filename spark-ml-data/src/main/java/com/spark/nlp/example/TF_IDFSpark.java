package com.spark.nlp.example;


import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.Vector;
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
 * https://blog.csdn.net/liulingyuan6/article/details/53381122
 *
 * https://time.geekbang.org/column/intro/74 推荐学习视频
 */
public class TF_IDFSpark
{

    public static String SPARK_HOME = "spark://titanic:7077";

    public static void main(String[] args)
    {

        SparkSession spark = SparkSession.builder().
                master(SPARK_HOME).
                appName("TF_IDFSpark").
                getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "what is the weather like today"),
                RowFactory.create(0.0, "what is for dinner tonight"),
                RowFactory.create(1.0, "this is a question worth pondering"),
                RowFactory.create(1.0, "it is a beautiful day today")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
        sentenceData.show(false);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);
        wordsData.show(10,false);

        int numFeatures = 20;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);
        Dataset<Row> featurizedData = hashingTF.transform(wordsData);

        featurizedData.show(false);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);

        Dataset<Row> rescaledData = idfModel.transform(featurizedData);

        rescaledData.show(false);

        for (Row r : rescaledData.select("features", "label").takeAsList(4)) {
            Vector features = r.getAs(0);
            Double label = r.getDouble(1);
            System.out.println(features);
            System.out.println(label);
        }

        spark.stop();
    }
}
