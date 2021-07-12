package com.spark.nlp.splitword;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
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
 *  Pipeline构建3个算法 文本进行二分类
 * 1.分词
 * 2.将词转换为向量
 * 3.使用逻辑回归进行二分类
 */
public class SparkTokenizerExampl
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("DescribeExampleSpark")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1L, 1.0, "spark rocks"),
                RowFactory.create(2L, 0.0, "flink is the best"),
                RowFactory.create(3L, 1.0, "Spark rules"),
                RowFactory.create(4L, 0.0, "mapreduce forever"),
                RowFactory.create(5L, 0.0, "Kafka is great")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("words", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> trainset = spark.createDataFrame(data, schema);

        //创建分词器
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("words")
                .setOutputCol("tokens");

        //创建HashingTF ,将词转换为特征向量
        HashingTF features = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");

        //使用逻辑回归创建一个模型，预测一篇新文档属于哪一组
        LogisticRegression logisticRegression = new LogisticRegression()
                .setMaxIter(15)
                .setRegParam(0.01);

        //根据一个包括3个stage的数组构造一个数据管道
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, features, logisticRegression});

        //训练模型
        PipelineModel model = pipeline.fit(trainset);


        //创建测试数据集
        List<Row> testData = Arrays.asList(
                RowFactory.create(10L, 1.0, "use spark please"),
                RowFactory.create(11L, 2.0, "Kafka")

        );

        StructType testSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("words", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> testSet = spark.createDataFrame(testData, testSchema);

        Dataset<Row> transform = model.transform(testSet);
        transform.show(false);
        transform.select("probability","prediction").show(false);
        spark.stop();
    }
}
