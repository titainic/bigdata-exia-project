package com.spark.ml.example;

import org.apache.spark.ml.regression.LinearRegression;

/**
 * L2岭回归示例
 * https://stackoverflow.com/questions/37639709/how-to-use-l1-regularization-for-logisticregressionwithlbfgs-in-spark-mllib
 *
 */
public class RidgeRegressionExample
{
    public static void main(String[] args)
    {
        LinearRegression lr = new LinearRegression();
        /**
         * 设置ElasticNet混合参数。当= 0时，惩罚是L2。等于= 1，它是一个L1。等于 0 < alpha < 1，惩罚是L1和L2的组合。默认值0.0是L2惩罚。
         */
        lr.setElasticNetParam(2.0);


    }
}
