package com.nordstrom.product.etl.spark.dao;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by bmwi on 4/4/17.
 */
public interface IBaseDao {

    JavaSparkContext getJavaSparkContext();

}
