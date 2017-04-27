package com.nordstrom.product.etl.spark.dao;

import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.json.simple.JSONObject;

import java.util.Map;

/**
 * Created by bmwi on 3/15/17.
 */
public interface ISkuDao extends IBaseDao{
    void saveDropShipEligibleFile(String keyspace, String[] columns, Function<CassandraRow, Boolean> filterfunction,
                                  Function<CassandraRow, JSONObject> mappingfunction, String location);
}
