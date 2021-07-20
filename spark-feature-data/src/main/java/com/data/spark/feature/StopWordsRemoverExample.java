package com.data.spark.feature;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.optimizer.SimpleTestOptimizer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * 停用词
 * 删除停用词
 * 不支持中文
 */
public class StopWordsRemoverExample
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder().
                master("spark://titanic:7077").
                appName("Tokenizer").
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


        //分词
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("sentence")
                .setOutputCol("words");


        Dataset<Row> wordsData = tokenizer.transform(sentenceData);

        StopWordsRemover swr = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("output");

        Dataset<Row> stopDS = swr.transform(wordsData);

        stopDS.show(false);

        spark.stop();
    }
}
