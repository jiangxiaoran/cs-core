package com.aia.gdp.model;

import com.aia.gdp.service.JobControlService.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 作业组数据对象（整合状态信息）
 * 将作业组定义数据和运行状态整合在一起
 * 
 * @author andy
 * @date 2025-08-14
 */
public class JobGroupData {
    private static final Logger logger = LoggerFactory.getLogger(JobGroupData.class);
    
    private final String groupName;
    private final CopyOnWriteArrayList<JobDef> jobs;  // 🎯 使用线程安全的CopyOnWriteArrayList
    private volatile ExecutionStatus status;   // 新增：组状态
    private volatile boolean paused;           // 新增：暂停标志
    private volatile boolean stopped;          // 新增：停止标志
    private volatile boolean cancelled;        // 新增：取消标志
    private volatile long lastUpdateTime;      // 新增：最后更新时间
    private final ReentrantReadWriteLock lock; // 新增：状态锁
    
    public JobGroupData(String groupName, List<JobDef> jobs) {
        this.groupName = groupName;
        this.jobs = new CopyOnWriteArrayList<>(jobs);  // 🎯 包装为线程安全列表
        this.status = ExecutionStatus.PENDING;
        this.lock = new ReentrantReadWriteLock();
        this.lastUpdateTime = System.currentTimeMillis();
    }
    
    /**
     * 更新组状态
     */
    public boolean updateStatus(ExecutionStatus newStatus) {
        lock.writeLock().lock();
        try {
            ExecutionStatus oldStatus = this.status;
            this.status = newStatus;
            this.lastUpdateTime = System.currentTimeMillis();
            
            // 自动清理相关标志
            updateFlags(newStatus);
            
            logger.debug("作业组 {} 状态从 {} 更新为 {}", groupName, oldStatus, newStatus);
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
    public List<JobDef> getJobs() { return jobs; }
    public int getJobCount() { return jobs.size(); }
    public String getGroupName() { return groupName; }
    
    // 新增的状态访问方法
    public ExecutionStatus getStatus() { return status; }
    public boolean isPaused() { return paused; }
    public boolean isStopped() { return stopped; }
    public boolean isCancelled() { return cancelled; }
    public long getLastUpdateTime() { return lastUpdateTime; }
    
    /**
     * 从作业组中移除指定作业
     */
    public boolean removeJob(String jobCode) {
        lock.writeLock().lock();
        try {
            // 🎯 使用CopyOnWriteArrayList的线程安全移除方法
            boolean removed = false;
            for (int i = 0; i < jobs.size(); i++) {
                JobDef job = jobs.get(i);
                if (jobCode.equals(job.getJobCode())) {
                    jobs.remove(i);
                    removed = true;
                    break;
                }
            }
            
            if (removed) {
                this.lastUpdateTime = System.currentTimeMillis();
                logger.debug("从作业组 {} 中移除作业: {}", groupName, jobCode);
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 检查作业组是否为空（没有作业）
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return jobs.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取组内作业状态统计
     */
    public GroupStatusStatistics getStatusStatistics() {
        // 这里需要外部传入作业状态，因为JobGroupData本身不包含作业状态
        // 暂时返回空统计，后续可以通过依赖注入或其他方式获取
        return new GroupStatusStatistics();
    }
    
    /**
     * 状态统计内部类
     */
    public static class GroupStatusStatistics {
        private final long timestamp;
        
        public GroupStatusStatistics() {
            this.timestamp = System.currentTimeMillis();
        }
        
        public long getTimestamp() { return timestamp; }
        
        // 可以后续扩展更多统计信息
    }
}
