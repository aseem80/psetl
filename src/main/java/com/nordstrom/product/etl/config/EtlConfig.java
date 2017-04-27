package com.nordstrom.product.etl.config;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;

import javax.annotation.Resource;
import java.util.*;

/**
 * This is Factory class hides all necessary complexity to connect to Spark
 * cluster. It creates SparkContext for distributed as well as local in memory
 * spark cluster. All application code and test code should use it to submit
 * Spark job.
 */
@Configuration
@ComponentScan("com.nordstrom.product.etl")
@PropertySource("classpath:application.properties")
public class EtlConfig {

    private static final Logger LOGGER = Logger.getLogger(EtlConfig.class);

    private static final String SPARK_PROPERTY_PREFIX = "spark";
    private static final String SPARK_SQL_PROPERTY_PREFIX = "spark.sql";
    private static final String HDFS_PROPERTY_PREFIX="fs.";

    private static final List<String> ENVIRONMENTAL_SPARK_PROPERTIES = new ArrayList<>();

    static {
        ENVIRONMENTAL_SPARK_PROPERTIES.add("spark.master.url");
        ENVIRONMENTAL_SPARK_PROPERTIES.add("spark.home");
        ENVIRONMENTAL_SPARK_PROPERTIES.add("spark.cassandra.connection.host");
    }




    @Resource(name = "etlAppProperties")
    private Map<String, String> etlAppProperties;

    @Autowired
    private Environment env;

    @Value("${spark.master.url}")
    private String sparkMaster;


    @Value("#{'${ps.etl.tasks.jar}'.split(',')}")
    private List<String> sparkJars;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${spark.cassandra.connection.host}")
    private String cassandraHost;



    @Bean(name = "etlAppProperties")
    public static PropertiesFactoryBean mapper() {
        PropertiesFactoryBean bean = new PropertiesFactoryBean();
        bean.setLocation(new ClassPathResource(
                "application.properties"));
        return bean;
    }


    @Bean
    public SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(sparkMaster);
        sparkConf.setJars(sparkJars.toArray(new String[sparkJars.size()]));
        if(StringUtils.contains(sparkMaster, "local")) {
            sparkConf.setSparkHome(sparkHome);
        }
        sparkConf.set("spark.cassandra.connection.host", cassandraHost);

        Set<String> properties = etlAppProperties.keySet();
        for (String property : properties) {
            if (!ENVIRONMENTAL_SPARK_PROPERTIES.contains(property) && StringUtils.startsWith(property,
                    SPARK_PROPERTY_PREFIX) && !StringUtils.startsWith(property, SPARK_SQL_PROPERTY_PREFIX)) {
                sparkConf.set(property, env.resolvePlaceholders(etlAppProperties.get(property)));
            }
        }
        return sparkConf;
    }



}
