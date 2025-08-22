package com.aia.gdp.dto;

import java.util.List;
import java.util.Map;

/**
 * 批量操作请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class BatchJobRequest {
    private List<Long> jobIds;
    private Map<String, Object> params;

    public BatchJobRequest() {
    }

    public BatchJobRequest(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public BatchJobRequest(List<Long> jobIds, Map<String, Object> params) {
        this.jobIds = jobIds;
        this.params = params;
    }

    // Getters and Setters
    public List<Long> getJobIds() {
        return jobIds;
    }

    public void setJobIds(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
} 