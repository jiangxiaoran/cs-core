package com.aia.gdp.dto;

/**
 * 任务列表查询请求DTO
 * 
 * @author andy
 * @date 2025-08-07
 */
public class JobListRequest {
    private Integer current = 1;
    private Integer pageSize = 10;
    private String jobCode;
    private String jobName;
    private String jobGroup;
    private String status;
    private String jobType;
    private Boolean isActive;

    public JobListRequest() {
    }

    public JobListRequest(Integer current, Integer pageSize) {
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

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }
} 