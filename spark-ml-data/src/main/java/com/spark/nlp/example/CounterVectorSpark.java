package com.spark.nlp.example;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

public class CounterVectorSpark
{

    public static String SPARK_HOME = "spark://titanic:7077";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("CountVectorizer")
                .master(SPARK_HOME)
                .getOrCreate();
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("a", "b", "c")),
                RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
        );
        StructType schema = new StructType(new StructField [] {
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

// fit a CountVectorizerModel from the corpus
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setVocabSize(3)
                .setMinDF(2)
                .fit(df);

// alternatively, define CountVectorizerModel with a-priori vocabulary
        CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
                .setInputCol("text")
                .setOutputCol("feature");

        cvModel.transform(df).show(false);
    }
}
