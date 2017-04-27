package com.nordstrom.product.etl.controller;

import com.nordstrom.product.etl.service.ISkuService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by bmwi on 4/10/17.
 */

@RequestMapping("/sku")
@RestController
public class SkuController {

    private static final String HOME_PAGE = "Home Page for Spark Driver program";

    @Autowired
    private ISkuService skuService;

    @RequestMapping(value = "/dopShipEligibleFile", method= RequestMethod.POST)
    public void generatedopShipEligibleFile () {
        skuService.saveDropShipEligibleFile();

    }

    @RequestMapping(value = "/home", method= RequestMethod.GET)
    public String home () {
        return HOME_PAGE;
    }

}
