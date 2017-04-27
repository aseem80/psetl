package com.nordstrom.product.etl.service;

import com.nordstrom.product.etl.spark.dao.ISkuDao;
import com.nordstrom.product.etl.spark.task.CassandraRowFilterFunction;
import com.nordstrom.product.etl.spark.task.CassandraRowToJavaRowMapFunction;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by bmwi on 3/15/17.
 */
@Service
public class SkuService implements ISkuService {

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final String[] SELECTED_COLUMNS = new String[]{"sku_Id","sku_VendorUpcs","market_Code","catalog",
            "sku_AuditControl", "sku_ProductAction", "sku_Description","sku_ItemTypeDescription",
            "sku_ShortDescription","sku_FormulationCode","sku_IsCustomizationRequired","sku_FragranceFamilies",
            "sku_ProductCatalogProvider","sku_RgbColorGreenValue","sku_AutoRemindFulfill","sku_LaptopCapacityDescription" };

    static {
        SIMPLE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private static final Logger LOGGER = Logger.getLogger(SkuService.class);



    @Autowired
    private ISkuDao skuDao;



    @Value("${cassandra.keyspace:nps}")
    private String keyspace;

    @Value("${ps.etl.dropShipEligibleFileLocation.prefix}")
    private String locationPrefix;



    @Scheduled(cron = "${ps.etl.dropShipFileCronTabExpression}")
    public void saveDropShipEligibleFile() {

        LOGGER.info("Submitting spark job and generating file");

        String location = locationPrefix + SIMPLE_DATE_FORMAT.format(new Date());
        skuDao.saveDropShipEligibleFile(keyspace, SELECTED_COLUMNS, new
                CassandraRowFilterFunction(), new CassandraRowToJavaRowMapFunction(),location);

    }

}
