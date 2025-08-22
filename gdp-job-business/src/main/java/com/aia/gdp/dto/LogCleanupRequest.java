package com.aia.gdp.dto;

import java.util.Date;
import java.util.List;

/**
 * 日志清理请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class LogCleanupRequest {
    private Date beforeDate;
    private List<String> status;
    private List<String> jobCodes;

    public LogCleanupRequest() {
    }

    public LogCleanupRequest(Date beforeDate, List<String> status, List<String> jobCodes) {
        this.beforeDate = beforeDate;
        this.status = status;
        this.jobCodes = jobCodes;
    }

    public Date getBeforeDate() {
        return beforeDate;
    }

    public void setBeforeDate(Date beforeDate) {
        this.beforeDate = beforeDate;
    }

    public List<String> getStatus() {
        return status;
    }

    public void setStatus(List<String> status) {
        this.status = status;
    }

    public List<String> getJobCodes() {
        return jobCodes;
    }

    public void setJobCodes(List<String> jobCodes) {
        this.jobCodes = jobCodes;
    }
} 