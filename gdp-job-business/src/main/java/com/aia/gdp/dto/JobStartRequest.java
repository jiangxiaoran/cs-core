package com.aia.gdp.dto;

import java.util.Map;

/**
 * 任务启动请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class JobStartRequest {
    private Map<String, Object> params;

    public JobStartRequest() {
    }

    public JobStartRequest(Map<String, Object> params) {
        this.params = params;
    }

    // Getters and Setters
    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
} 