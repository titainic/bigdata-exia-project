package com.spark.hive.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Spark3.0.0读取hive3.1.2 分区表数据
 */
public class SparkSqlReadHiveV3 {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://titanic:7077")
                .appName("jiaotou")
                .enableHiveSupport()    //支持hive
                .config("hive.metastore.uris", "thrift://titanic:9083") //设置hive metastore
                .getOrCreate();

        Dataset<Row> hiveDataDS = spark.sql("select * from ods_gantry_transaction");
        hiveDataDS.show(false);

        spark.stop();

    }


}
