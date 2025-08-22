package com.aia.gdp.dto;

import java.util.List;

/**
 * 批量删除执行日志请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class BatchLogDeleteRequest {
    private List<Long> logIds;

    public BatchLogDeleteRequest() {
    }

    public BatchLogDeleteRequest(List<Long> logIds) {
        this.logIds = logIds;
    }

    public List<Long> getLogIds() {
        return logIds;
    }

    public void setLogIds(List<Long> logIds) {
        this.logIds = logIds;
    }
} 