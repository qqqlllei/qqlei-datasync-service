package com.qqlei.shop.datasync.service;

import com.qqlei.shop.datasync.service.fallback.ProductServiceFallback;
import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "qqlei-product-service",fallback = ProductServiceFallback.class)
public interface ProductService {

    @RequestMapping(value = "/brand/findById",method = RequestMethod.GET)
    String findBrandById(@RequestParam(value = "id") Long id);
    
    @RequestMapping(value = "/category/findById",method = RequestMethod.GET)
    String findCategoryById(@RequestParam(value = "id") Long id);
    
    @RequestMapping(value = "/product-intro/findById",method = RequestMethod.GET)
    String findProductIntroById(@RequestParam(value = "id") Long id);
    
    @RequestMapping(value = "/product-property/findById",method = RequestMethod.GET)
    String findProductPropertyById(@RequestParam(value = "id") Long id);
    
    @RequestMapping(value = "/product/findById",method = RequestMethod.GET)
    String findProductById(@RequestParam(value = "id") Long id);
    
    @RequestMapping(value = "/product-specification/findById",method = RequestMethod.GET)
    String findProductSpecificationById(@RequestParam(value = "id") Long id);

}