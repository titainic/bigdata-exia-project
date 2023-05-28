package com.spark.hive.example;

import org.apache.spark.sql.AnalysisException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.to_date;


/**
 * Spark3.0.0写入hive3.1.2 分区表数据
 */
public class SparkSqlWriteHiveV3 {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("jiaotou")
                .enableHiveSupport()
                .config("hive.metastore.uris", "thrift://titanic:9083")
                .getOrCreate();


        Dataset<Row> gantryTransactionDS = spark.read()
                                                .option("header", true)
                                                .csv("spark-hive-3/src/main/resources/liu.csv");

        Dataset<Row> carDate = gantryTransactionDS.select(to_date(col("TransTime")).as("TransTime"), col("GantryId"), col("MediaType"), col("PayFee").cast(DataTypes.IntegerType), col("SpecialType")).where(month(col("TransTime")).isin(2)).repartition(1);

        carDate.createTempView("transaction_tmp");

        spark.sql("insert into ods_gantry_transaction partition(month='202102') select GantryId,MediaType,TransTime,PayFee,SpecialType from transaction_tmp ");

        spark.stop();

    }


}
