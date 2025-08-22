package com.aia.gdp;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@MapperScan(basePackages = "com.aia.gdp.mapper")
@EnableAsync
public class GDPJobBusinessApplication {
    public static void main(String[] args) {
        SpringApplication.run(GDPJobBusinessApplication.class, args);
    }
} 