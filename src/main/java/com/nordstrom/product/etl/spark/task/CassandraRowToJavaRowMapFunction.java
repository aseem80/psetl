package com.nordstrom.product.etl.spark.task;

import com.datastax.spark.connector.CassandraRowMetadata;
import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;
import scala.collection.IndexedSeq;
import scala.collection.JavaConversions;

import java.util.List;


/**
 * Created by bmwi on 3/15/17.
 */
public class CassandraRowToJavaRowMapFunction implements Function<CassandraRow, JSONObject> {


    @Override
    public JSONObject call(CassandraRow cassandraRow) throws Exception {
        JSONObject result = new JSONObject();
        CassandraRowMetadata metaData = cassandraRow.fieldNames();
        IndexedSeq<String> seq = metaData.columnNames();
        List<String> columns = JavaConversions.seqAsJavaList(seq);
        for (String column : columns) {
            result.put(column, cassandraRow.getObject(column));
        }
        return result;
    }


}
