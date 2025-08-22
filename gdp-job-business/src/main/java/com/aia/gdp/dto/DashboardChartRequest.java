package com.aia.gdp.dto;

/**
 * 仪表板图表请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class DashboardChartRequest {
    private String startDate;
    private String endDate;
    private String groupBy = "day";
    private String jobCode;
    private Integer limit = 10;

    public DashboardChartRequest() {
    }

    public DashboardChartRequest(String startDate, String endDate, String groupBy, String jobCode) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.groupBy = groupBy;
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

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public String getJobCode() {
        return jobCode;
    }

    public void setJobCode(String jobCode) {
        this.jobCode = jobCode;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit != null ? Math.min(limit, 50) : 10;
    }
} 