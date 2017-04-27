package com.nordstrom.product.etl.spark.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by bmwi on 4/4/17.
 */

@Repository
public class BaseDao implements IBaseDao{


    private static final String HDFS_PROPERTY_PREFIX="fs.";

    private static final List<String> ENVIRONMENTAL_HDFS_PROPERTIES = new ArrayList<>();




    @Autowired
    protected SparkConf sparkConf;

    @Resource(name = "etlAppProperties")
    protected Map<String, String> etlAppProperties;

    @Autowired
    private Environment env;

    public JavaSparkContext getJavaSparkContext() {

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Set<String> properties = etlAppProperties.keySet();

        for (String property : properties) {
            if (!ENVIRONMENTAL_HDFS_PROPERTIES.contains(property) && StringUtils.startsWith(property,
                    HDFS_PROPERTY_PREFIX)) {
                sc.hadoopConfiguration().set(property, env.resolvePlaceholders(etlAppProperties.get(property)));
            }
        }
        return sc;

    }

}
