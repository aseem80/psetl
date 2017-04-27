package com.nordstrom.product.etl.spark.task;

import com.datastax.spark.connector.japi.CassandraRow;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.spark.api.java.function.Function;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordstrom.tds.pvo.module.product.model.v4.VendorUpcSku;
import com.nordstrom.tds.pvo.module.product.model.v4.VendorSku;
import java.util.List;
import java.util.Optional;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Created by bmwi on 3/21/17.
 */
public class CassandraRowFilterFunction implements Function<CassandraRow, Boolean>, java.io.Serializable {

    private static final String DROPSHIP_ELIGIBLE_COLUMN = "sku_VendorUpcs";


    @Override
    public Boolean call(CassandraRow cassandraRow) throws Exception {
        Boolean isDropShipEligible = false;
        String vendorUpcsValue = cassandraRow.getString(DROPSHIP_ELIGIBLE_COLUMN);
        if (null != vendorUpcsValue && !vendorUpcsValue.isEmpty()) {
            ObjectMapper mapper = new ObjectMapper();
            mapper = mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false).configure
                    (DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            List<VendorUpcSku> vendorUpcSkus = mapper.readValue(vendorUpcsValue, new
                    TypeReference<List<VendorUpcSku>>() {
                    });
            Optional<VendorUpcSku> vendorUpcSkuOptional = vendorUpcSkus.stream().filter(vendorUpcSku -> {
                List<VendorSku> vendorSkus = vendorUpcSku.getVendors();

                Optional<VendorSku> vendorSkuOptional = vendorSkus.stream().filter(vendorSku-> vendorSku
                        .getIsDropShipEligible()).findAny();
                if(vendorSkuOptional.isPresent()) {
                    return true;
                } else {
                    return false;
                }

            }).findAny();
            if(vendorUpcSkuOptional.isPresent()){
                return true;
            }
        }
        return isDropShipEligible;

    }
}
