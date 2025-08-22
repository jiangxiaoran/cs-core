package com.aia.gdp.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 测试控制器
 * 用于调试静态资源问题
 * 
 * @author andy
 * @date 2025-08-14
 */
@RestController
@RequestMapping("/test")
public class TestController {
    
    /**
     * 测试JS文件访问
     */
    @GetMapping("/js")
    public ResponseEntity<String> testJs() {
        try {
            ClassPathResource resource = new ClassPathResource("static/umi.921a2fe9.js");
            if (resource.exists()) {
                String content = new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                return ResponseEntity.ok()
                        .contentType(MediaType.valueOf("application/javascript"))
                        .body(content);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body("Error reading file: " + e.getMessage());
        }
    }
    
    /**
     * 测试静态资源路径
     */
    @GetMapping("/static-info")
    public ResponseEntity<String> staticInfo() {
        try {
            ClassPathResource resource = new ClassPathResource("static/");
            if (resource.exists()) {
                return ResponseEntity.ok("Static directory exists: " + resource.getPath());
            } else {
                return ResponseEntity.ok("Static directory not found");
            }
        } catch (Exception e) {
            return ResponseEntity.ok("Error checking static directory: " + e.getMessage());
        }
    }
}
