package com.aia.gdp.dto;

import java.util.Date;
import java.util.List;

/**
 * 日志导出请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class LogExportRequest {
    private Date startDate;
    private Date endDate;
    private List<String> jobCodes;
    private List<String> status;
    private String format = "excel";

    public LogExportRequest() {
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public List<String> getJobCodes() {
        return jobCodes;
    }

    public void setJobCodes(List<String> jobCodes) {
        this.jobCodes = jobCodes;
    }

    public List<String> getStatus() {
        return status;
    }

    public void setStatus(List<String> status) {
        this.status = status;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
} 