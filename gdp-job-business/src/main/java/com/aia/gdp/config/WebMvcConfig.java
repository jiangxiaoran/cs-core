package com.aia.gdp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web MVC 配置
 * 
 * 功能：
 * - 配置静态资源路径
 * - 支持 SPA 应用
 * 
 * @author andy
 * @date 2025-08-14
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
    
    @Value("${app.frontend.static-path:/static}")
    private String staticPath;
    
    @Value("${app.frontend.base-path:/web}")
    private String basePath;
    
    /**
     * 配置静态资源处理器
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // 配置前端静态资源路径 - 优先级最高
        registry.addResourceHandler("/**")
                .addResourceLocations("classpath:/static/");
    }
}
