package com.aia.gdp.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 健康检查控制器
 * 
 * 功能：
 * - 提供系统健康检查
 * - 提供系统信息
 * - 监控系统状态
 * 
 * @author andy
 * @date 2025-08-14
 */
@RestController
public class HealthController {
    
    /**
     * 健康检查接口
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }
    
    /**
     * 系统信息接口
     */
    @GetMapping("/api/system/info")
    public ResponseEntity<Map<String, Object>> getSystemInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("name", "友邦报表任务统一处理平台");
        info.put("version", "1.0.0");
        info.put("status", "running");
        info.put("frontend", "Ant Design Pro + UmiJS");
        info.put("backend", "Spring Boot");
        info.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(info);
    }
    
    /**
     * 前端应用状态检查
     */
    @GetMapping("/api/frontend/status")
    public ResponseEntity<Map<String, Object>> getFrontendStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "ready");
        status.put("framework", "React + Ant Design Pro");
        status.put("buildTool", "UmiJS");
        status.put("routing", "SPA");
        
        return ResponseEntity.ok(status);
    }
}
