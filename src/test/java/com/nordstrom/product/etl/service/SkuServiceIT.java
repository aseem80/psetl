package com.nordstrom.product.etl.service;


import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordstrom.product.etl.config.EtlTestConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bmwi on 3/23/17.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = EtlTestConfig.class)
@TestPropertySource("classpath:application-test.properties")
@EnableAutoConfiguration
public class SkuServiceIT {


    private static final Integer SLEEPTIME_IN_MILLISECONDS = 15000;
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    static {
        SIMPLE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    @Value("${ps.etl.cassandra.table.ddl}")
    private String ddl;

    @Autowired
    public Session session;

    @Autowired
    private ISkuService skuService;

    @Autowired
    private CassandraOperations template;


    @Value("${ps.etl.dropShipEligibleFileLocation.prefix}")
    private String outputFilePath;

    @Autowired
    private ObjectMapper mapper;





    @Before
    public void setUp() {
        assertTrue(createSkuTable().wasApplied());
        assertTrue(insertTestData().wasApplied());

    }

    @Test
    public void saveDropShipEligibleFile() throws Exception{
        skuService.saveDropShipEligibleFile();
        Thread.sleep(SLEEPTIME_IN_MILLISECONDS);
        File directory = Paths.get(StringUtils.substringBeforeLast(outputFilePath, "/")).toFile();
        File[] folders = directory.listFiles();
        assertEquals(1, folders.length);
        File dropShipFolder = folders[0];
        String expectedName = StringUtils.substringAfterLast(outputFilePath, "/") + SIMPLE_DATE_FORMAT.format(new Date());
        assertEquals(expectedName, dropShipFolder.getName());
        File[] sparkGeneratedDataFiles = dropShipFolder.listFiles();
        assertTrue(sparkGeneratedDataFiles.length > 0);

        Optional<File> isSuccessFileGenerated =  Arrays.stream(sparkGeneratedDataFiles).filter(file -> FilenameUtils
                .getName
                (file
                .toPath().toString()).equalsIgnoreCase("_SUCCESS")).findAny();
        assertTrue(isSuccessFileGenerated.isPresent());


        Optional<File> isDataFileGenerated =

        Arrays.stream(sparkGeneratedDataFiles).filter(file -> StringUtils.startsWith(FilenameUtils.getName(file
                .toPath().toString()),"part")
        ).findAny();

        assertTrue(isDataFileGenerated.isPresent());
        File dataFile = isDataFileGenerated.get();

        List<String> lines= new ArrayList<>();

        List<Object> validSkuIds = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        }
        assertEquals(2,lines.size());
        lines.stream().forEach(line -> {TypeReference<HashMap<String,Object>> typeRef
                = new TypeReference<HashMap<String,Object>>() {};
            try {
                HashMap<String,Object> map = mapper.readValue(line, typeRef);
                validSkuIds.add(map.get("sku_Id"));

            } catch (IOException e) {
                Assert.fail("Not Expecting exception since it is valoid json");
            }

        });
        assertEquals(2, validSkuIds.size());
        assertTrue(validSkuIds.stream().filter(skuId-> skuId.toString().equals("2")).findFirst().isPresent());
        assertTrue(validSkuIds.stream().filter(skuId-> skuId.toString().equals("1")).findFirst().isPresent());

    }



    @After
    public void dropTable() throws Exception{
        template.execute("TRUNCATE sku");
        FileUtils.deleteDirectory(Paths.get(StringUtils.substringBeforeLast(outputFilePath, "/")).toFile());
    }


    private ResultSet insertTestData() {
        BatchStatement batch = new BatchStatement();
        PreparedStatement ps = session.prepare("Insert into sku(\"sku_Id\",\"market_Code\",\"catalog\", " +
                "\"sku_VendorUpcs\")  VALUES (?, ?, ?, ?)");
        batch.add(ps.bind(1L, "US", "Online", "[{\"Upcs\":[null],\"Vendors\":[{\"CostAmounts\":[{\"Amount\":57," +
                "\"CurrencyCode\":\"USD\"}],\"IsDropShipEligible\":true,\"IsNpgVendor\":false,\"IsPrimary\":true," +
                "\"IsRtvAllowed\":false,\"Name\":\"VINCE\",\"Number\":\"707824143\",\"OrderQuantityMultiple\":1," +
                "\"ActionTimestamp\":\"2017-01-11T14:36:39.09\",\"UnitOfPurchaseContainerTypeCode\":\"EA\"," +
                "\"VendorRoles\":[{\"Code\":\"SUP\",\"Description\":\"Supplier\"}]," +
                "\"VendorColorDescription\":\"WISTERIA\",\"VendorProductNumbers\":[{\"Code\":\"V193320836\"}]," +
                "\"VendorSizeDescription\":\"12\",\"CountriesOfAssembly\":[{\"CountryCode\":\"USA\"," +
                "\"CountryName\":\"UNITED STATES\",\"IsoShortCode\":\"USA\",\"IsPrimary\":true}]," +
                "\"CountriesOfOrigin\":[{\"CountryCode\":\"USA\",\"CountryName\":\"UNITED STATES\"," +
                "\"IsoShortCode\":\"USA\",\"IsPrimary\":true}]}]}]"));
        batch.add(ps.bind(2L, "CA", "Online", "[{\"Upcs\":[null],\"Vendors\":[{\"CostAmounts\":[{\"Amount\":57," +
                "\"CurrencyCode\":\"USD\"}],\"IsDropShipEligible\":true,\"IsNpgVendor\":false,\"IsPrimary\":true," +
                "\"IsRtvAllowed\":false,\"Name\":\"VINCE\",\"Number\":\"707824143\",\"OrderQuantityMultiple\":1," +
                "\"ActionTimestamp\":\"2017-01-11T14:36:39.09\",\"UnitOfPurchaseContainerTypeCode\":\"EA\"," +
                "\"VendorRoles\":[{\"Code\":\"SUP\",\"Description\":\"Supplier\"}]," +
                "\"VendorColorDescription\":\"WISTERIA\",\"VendorProductNumbers\":[{\"Code\":\"V193320836\"}]," +
                "\"VendorSizeDescription\":\"12\",\"CountriesOfAssembly\":[{\"CountryCode\":\"USA\"," +
                "\"CountryName\":\"UNITED STATES\",\"IsoShortCode\":\"USA\",\"IsPrimary\":true}]," +
                "\"CountriesOfOrigin\":[{\"CountryCode\":\"USA\",\"CountryName\":\"UNITED STATES\"," +
                "\"IsoShortCode\":\"USA\",\"IsPrimary\":true}]}]}]"));
        batch.add(ps.bind(3L, "US", "Online", null));
        batch.add(ps.bind(4L, "US", "Online", "[{\"Upcs\":[null],\"Vendors\":[{\"CostAmounts\":[{\"Amount\":57," +
                "\"CurrencyCode\":\"USD\"}],\"IsDropShipEligible\":false,\"IsNpgVendor\":false,\"IsPrimary\":true," +
                "\"IsRtvAllowed\":false,\"Name\":\"VINCE\",\"Number\":\"707824143\",\"OrderQuantityMultiple\":1," +
                "\"ActionTimestamp\":\"2017-01-11T14:36:39.09\",\"UnitOfPurchaseContainerTypeCode\":\"EA\"," +
                "\"VendorRoles\":[{\"Code\":\"SUP\",\"Description\":\"Supplier\"}]," +
                "\"VendorColorDescription\":\"WISTERIA\",\"VendorProductNumbers\":[{\"Code\":\"V193320836\"}]," +
                "\"VendorSizeDescription\":\"12\",\"CountriesOfAssembly\":[{\"CountryCode\":\"USA\"," +
                "\"CountryName\":\"UNITED STATES\",\"IsoShortCode\":\"USA\",\"IsPrimary\":true}]," +
                "\"CountriesOfOrigin\":[{\"CountryCode\":\"USA\",\"CountryName\":\"UNITED STATES\"," +
                "\"IsoShortCode\":\"USA\",\"IsPrimary\":true}]}]}]"));

        return session.execute(batch);
    }



    private ResultSet createSkuTable() {
        System.out.println("Executing DDL");
        ResultSet resultSet = session.execute(ddl);
        return resultSet;

    }



}
