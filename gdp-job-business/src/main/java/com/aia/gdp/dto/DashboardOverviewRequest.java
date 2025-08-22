package com.aia.gdp.dto;

/**
 * 仪表板概览请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class DashboardOverviewRequest {
    private String startDate;
    private String endDate;
    private String jobCode;

    public DashboardOverviewRequest() {
    }

    public DashboardOverviewRequest(String startDate, String endDate, String jobCode) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.jobCode = jobCode;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getJobCode() {
        return jobCode;
    }

    public void setJobCode(String jobCode) {
        this.jobCode = jobCode;
    }
} 