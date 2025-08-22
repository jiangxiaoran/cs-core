package com.aia.gdp.dto;

import java.util.Date;

/**
 * 执行日志列表查询请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class JobLogListRequest {
    private Integer current = 1;
    private Integer pageSize = 10;
    private String jobCode;
    private String batchNo;
    private String status;
    private Date startTime;
    private Date endTime;
    private String executorAddress;
    private String orderBy = "logId";  // 默认按logId排序
    private String orderDirection = "desc";  // 默认降序

    public JobLogListRequest() {
    }

    public JobLogListRequest(Integer current, Integer pageSize) {
        this.current = current;
        this.pageSize = pageSize;
    }

    // Getters and Setters
    public Integer getCurrent() {
        return current;
    }

    public void setCurrent(Integer current) {
        this.current = current != null ? current : 1;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize != null ? Math.min(pageSize, 100) : 10;
    }

    public String getJobCode() {
        return jobCode;
    }

    public void setJobCode(String jobCode) {
        this.jobCode = jobCode;
    }

    public String getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getExecutorAddress() {
        return executorAddress;
    }

    public void setExecutorAddress(String executorAddress) {
        this.executorAddress = executorAddress;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy != null ? orderBy : "logId";
    }

    public String getOrderDirection() {
        return orderDirection;
    }

    public void setOrderDirection(String orderDirection) {
        this.orderDirection = orderDirection != null ? orderDirection : "desc";
    }
} 