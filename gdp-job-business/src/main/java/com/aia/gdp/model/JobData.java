package com.aia.gdp.model;

import com.aia.gdp.service.JobControlService.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 作业数据对象（整合状态信息）
 * 将作业定义和运行状态整合在一起
 * 
 * @author andy
 * @date 2025-08-14
 */
public class JobData {
    private static final Logger logger = LoggerFactory.getLogger(JobData.class);
    
    private final JobDef jobDef;               // 原有的作业定义
    private volatile ExecutionStatus status;   // 新增：作业状态
    private volatile boolean paused;           // 新增：暂停标志
    private volatile boolean stopped;          // 新增：停止标志
    private volatile boolean cancelled;        // 新增：取消标志
    private volatile long lastUpdateTime;      // 新增：最后更新时间
    private long logId; // 日志id
    private final ReentrantReadWriteLock lock; // 新增：状态锁
    
    public JobData(JobDef jobDef) {
        this.jobDef = jobDef;
        this.status = ExecutionStatus.PENDING;
        this.lock = new ReentrantReadWriteLock();
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    /**
     * 更新作业状态
     */
    public boolean updateStatus(ExecutionStatus newStatus) {
        lock.writeLock().lock();
        try {
            ExecutionStatus oldStatus = this.status;
            this.status = newStatus;
            this.lastUpdateTime = System.currentTimeMillis();
            
            // 自动清理相关标志
            updateFlags(newStatus);
            
            logger.debug("作业 {} 状态从 {} 更新为 {}", jobDef.getJobCode(), oldStatus, newStatus);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 自动更新相关标志
     */
    private void updateFlags(ExecutionStatus newStatus) {
        switch (newStatus) {
            case PENDING:
                this.paused = false;      // PENDING 状态不是暂停状态
                this.stopped = false;
                this.cancelled = false;
                break;
            case RUNNING:
                this.paused = false;
                this.stopped = false;
                this.cancelled = false;
                break;
            case PAUSED:
                this.paused = true;
                this.stopped = false;
                this.cancelled = false;
                break;
            case STOPPED:
                this.paused = false;
                this.stopped = true;
                this.cancelled = false;
                break;
            case CANCELLED:
                this.paused = false;
                this.stopped = false;
                this.cancelled = true;
                break;
            case COMPLETED:
            case FAILED:
                this.paused = false;
                this.stopped = true;
                this.cancelled = false;
                break;
        }
    }
    
    // 原有的数据访问方法
    public JobDef getJobDef() { return jobDef; }
    public String getJobCode() { return jobDef.getJobCode(); }
    public String getJobName() { return jobDef.getJobName(); }
    public String getJobGroup() { return jobDef.getJobGroup(); }
    public String getProcName() { return jobDef.getProcName(); }
    public String getJobType() { return jobDef.getJobType(); }
    public Integer getJobOrder() { return jobDef.getJobOrder(); }
    public Integer getGroupOrder() { return jobDef.getGroupOrder(); }
    public String getJobParams() { return jobDef.getJobParams(); }
    public Integer getTimeoutSec() { return jobDef.getTimeoutSec(); }
    public Integer getRetryCount() { return jobDef.getRetryCount(); }
    public String getNotifyEmail() { return jobDef.getNotifyEmail(); }
    public Boolean getIsDepend() { return jobDef.getIsDepend(); }
    public Boolean getIsActive() { return jobDef.getIsActive(); }
    public long getLogId() { return logId; }
    public void setLogId(long logId) { this.logId = logId;  }

    // 新增的状态访问方法
    public ExecutionStatus getStatus() { return status; }
    public boolean isPaused() { return paused; }
    public boolean isStopped() { return stopped; }
    public boolean isCancelled() { return cancelled; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    
    /**
     * 检查作业是否可以执行
     */
    public boolean canExecute() {
        return status == ExecutionStatus.STOPPED || 
               status == ExecutionStatus.FAILED || 
               status == ExecutionStatus.COMPLETED;
    }
    
    /**
     * 检查作业是否可以暂停
     */
    public boolean canPause() {
        return status == ExecutionStatus.RUNNING || status == ExecutionStatus.PENDING;  // 允许运行中和等待中的任务暂停
    }
    
    /**
     * 检查作业是否可以恢复
     */
    public boolean canResume() {
        return status == ExecutionStatus.PAUSED;
    }
    
    /**
     * 检查作业是否可以停止
     */
    public boolean canStop() {
        return status == ExecutionStatus.RUNNING || status == ExecutionStatus.PAUSED;
    }
    
    /**
     * 检查作业是否可以取消
     */
    public boolean canCancel() {
        return status == ExecutionStatus.RUNNING || 
               status == ExecutionStatus.PAUSED || 
               status == ExecutionStatus.STOPPED || 
               status == ExecutionStatus.FAILED;
    }
}
