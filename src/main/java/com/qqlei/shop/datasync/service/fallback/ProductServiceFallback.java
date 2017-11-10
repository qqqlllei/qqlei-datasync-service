package com.qqlei.shop.datasync.service.fallback;

import com.qqlei.shop.datasync.service.ProductService;
import org.springframework.stereotype.Component;

/**
 * Created by 李雷 on 2017/11/9.
 */
@Component
public class ProductServiceFallback implements ProductService {
    @Override
    public String findBrandById(Long id) {

        System.out.println("======================[findBrandById-fallback]========================");
        return null;
    }

    @Override
    public String findCategoryById(Long id) {
        System.out.println("======================[findCategoryById-fallback]========================");
        return null;
    }

    @Override
    public String findProductIntroById(Long id) {
        System.out.println("======================[findProductIntroById-fallback]========================");
        return null;
    }

    @Override
    public String findProductPropertyById(Long id) {
        System.out.println("======================[findProductPropertyById-fallback]========================");
        return null;
    }

    @Override
    public String findProductById(Long id) {
        System.out.println("======================[findProductById-fallback]========================");
        return null;
    }

    @Override
    public String findProductSpecificationById(Long id) {
        System.out.println("======================[findProductSpecificationById-fallback]========================");
        return null;
    }
}
