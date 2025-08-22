package com.aia.gdp.model;

import java.time.LocalDateTime;

/**
 * 作业执行信息类
 * 用于事件监听器管理作业执行状态，不直接执行任务
 * 
 * @author andy
 * @date 2025-08-12
 */
public class JobExecutionInfo {
    
    private final String jobCode;
    private final String batchNo;
    private final String groupName;
    private volatile LocalDateTime startTime;
    
    public JobExecutionInfo(String jobCode, String batchNo, String groupName) {
        this.jobCode = jobCode;
        this.batchNo = batchNo;
        this.groupName = groupName;
        this.startTime = LocalDateTime.now();
    }
    
    // Getters
    public String getJobCode() { return jobCode; }
    public String getBatchNo() { return batchNo; }
    public String getGroupName() { return groupName; }
    public LocalDateTime getStartTime() { return startTime; }
    
    @Override
    public String toString() {
        return String.format("JobExecutionInfo{jobCode='%s', batchNo='%s', groupName='%s', startTime=%s}", 
                           jobCode, batchNo, groupName, startTime);
    }
}
