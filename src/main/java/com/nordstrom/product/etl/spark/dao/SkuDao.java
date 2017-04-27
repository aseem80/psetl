package com.nordstrom.product.etl.spark.dao;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.util.Map;

/**
 * Created by bmwi on 3/15/17.
 */

@Repository
public class SkuDao extends BaseDao implements ISkuDao {


    private static final String SKU_TABLE = "sku";


    @Value("${ps.etl.dropShipFile.output.partition}")
    private Integer repartition;



    @Override
    public void saveDropShipEligibleFile(String keyspace, String[] columns, Function<CassandraRow, Boolean> filterfunction,
                                         Function<CassandraRow, JSONObject> mappingfunction, String location) {
        JavaSparkContext sc = null;
        try {
            sc = getJavaSparkContext();
            CassandraJavaRDD<CassandraRow> cassandraRowRdd = CassandraJavaUtil.javaFunctions(sc).cassandraTable(keyspace,
                    SKU_TABLE);
            JavaRDD<CassandraRow> filteredJavaRDD = null;
            JavaRDD<JSONObject> jsonRdd = null;
            if (columns != null && columns.length > 0) {
                cassandraRowRdd = cassandraRowRdd.select(columns);
            }
            if (null != filterfunction) {
                filteredJavaRDD = cassandraRowRdd.filter(filterfunction);
                jsonRdd = filteredJavaRDD.map(mappingfunction);

            } else {
                jsonRdd = cassandraRowRdd.map(mappingfunction);
            }
            if(!jsonRdd.isEmpty()) {
                jsonRdd.repartition(repartition).saveAsTextFile(location);
            }

        } finally {
            if(sc!=null) {
                sc.stop();
            }

        }
    }
}
