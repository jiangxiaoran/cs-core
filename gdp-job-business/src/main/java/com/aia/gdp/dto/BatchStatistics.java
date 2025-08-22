package com.aia.gdp.dto;

import java.util.Date;

/**
 * 批次统计信息
 * 
 * @author andy
 * @date 2025-08-18
 */
public class BatchStatistics {
    private String batchNo;
    private int totalJobs;
    private int runningJobs;
    private int completedJobs;
    private int failedJobs;
    private int pausedJobs;
    private int stoppedJobs;
    private int cancelledJobs;
    private double successRate;
    private Date lastUpdateTime;
    private String dataSource; // 数据来源标识
    
    // 构造函数
    public BatchStatistics() {}
    
    public BatchStatistics(String batchNo) {
        this.batchNo = batchNo;
        this.lastUpdateTime = new Date();
    }
    
    // getter 和 setter 方法
    public String getBatchNo() { return batchNo; }
    public void setBatchNo(String batchNo) { this.batchNo = batchNo; }
    
    public int getTotalJobs() { return totalJobs; }
    public void setTotalJobs(int totalJobs) { this.totalJobs = totalJobs; }
    
    public int getRunningJobs() { return runningJobs; }
    public void setRunningJobs(int runningJobs) { this.runningJobs = runningJobs; }
    
    public int getCompletedJobs() { return completedJobs; }
    public void setCompletedJobs(int completedJobs) { this.completedJobs = completedJobs; }
    
    public int getFailedJobs() { return failedJobs; }
    public void setFailedJobs(int failedJobs) { this.failedJobs = failedJobs; }
    
    public int getPausedJobs() { return pausedJobs; }
    public void setPausedJobs(int pausedJobs) { this.pausedJobs = pausedJobs; }
    
    public int getStoppedJobs() { return stoppedJobs; }
    public void setStoppedJobs(int stoppedJobs) { this.stoppedJobs = stoppedJobs; }
    
    public int getCancelledJobs() { return cancelledJobs; }
    public void setCancelledJobs(int cancelledJobs) { this.cancelledJobs = cancelledJobs; }
    
    public double getSuccessRate() { return successRate; }
    public void setSuccessRate(double successRate) { this.successRate = successRate; }
    
    public Date getLastUpdateTime() { return lastUpdateTime; }
    public void setLastUpdateTime(Date lastUpdateTime) { this.lastUpdateTime = lastUpdateTime; }
    
    public String getDataSource() { return dataSource; }
    public void setDataSource(String dataSource) { this.dataSource = dataSource; }
    
    @Override
    public String toString() {
        return "BatchStatistics{" +
                "batchNo='" + batchNo + '\'' +
                ", totalJobs=" + totalJobs +
                ", runningJobs=" + runningJobs +
                ", completedJobs=" + completedJobs +
                ", failedJobs=" + failedJobs +
                ", pausedJobs=" + pausedJobs +
                ", stoppedJobs=" + stoppedJobs +
                ", cancelledJobs=" + cancelledJobs +
                ", successRate=" + successRate +
                ", lastUpdateTime=" + lastUpdateTime +
                ", dataSource='" + dataSource + '\'' +
                '}';
    }
}
