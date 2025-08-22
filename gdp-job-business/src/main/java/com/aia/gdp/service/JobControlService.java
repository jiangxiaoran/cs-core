package com.aia.gdp.service;

import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 作业控制服务接口
 * 提供作业队列的暂停、恢复、终止等控制功能
 * 
 * 重构说明：
 * - 新增支持批次号的方法，解决多次触发时的控制混乱问题
 * - 保持原有方法签名，确保向后兼容性
 * - 新增批次级别的数据管理和清理方法
 * 
 * @author andy
 * @date 2025-08-18
 * @version 2.0
 */
public interface JobControlService {
    
    /**
     * 执行状态枚举
     */
    enum ExecutionStatus {
        PENDING,    // 等待执行
        RUNNING,    // 运行中
        PAUSED,     // 已暂停
        STOPPED,    // 已停止
        COMPLETED,  // 已完成
        FAILED,     // 执行失败
        CANCELLED   // 已取消
    }
    
    /**
     * 控制操作枚举
     */
    enum ControlAction {
        PAUSE,      // 暂停
        RESUME,     // 恢复
        STOP,       // 停止
        RESTART,    // 重启
        CANCEL      // 取消
    }
    
    // ==================== 作业组控制（支持批次号） ====================
    
    /**
     * 暂停作业组（支持批次号）
     */
    boolean pauseJobGroup(String groupName, String batchNo);
    
    /**
     * 恢复作业组（支持批次号）
     */
    boolean resumeJobGroup(String groupName, String batchNo);
    
    /**
     * 停止作业组（支持批次号）
     */
    boolean stopJobGroup(String groupName, String batchNo);
    
    /**
     * 重启作业组（支持批次号）
     */
    boolean restartJobGroup(String groupName, String batchNo);
    
    /**
     * 取消作业组（支持批次号）
     */
    boolean cancelJobGroup(String groupName, String batchNo);
    
    /**
     * 更新作业组状态（支持批次号）
     */
    void updateGroupStatus(String groupName, String batchNo, String status);
    
    // ==================== 单个作业控制（支持批次号） ====================
    
    /**
     * 暂停单个作业（支持批次号）
     */
    boolean pauseJob(String jobCode, String batchNo);
    
    /**
     * 恢复单个作业（支持批次号）
     */
    boolean resumeJob(String jobCode, String batchNo);
    
    /**
     * 停止单个作业（支持批次号）
     */
    boolean stopJob(String jobCode, String batchNo);
    
    /**
     * 取消单个作业（支持批次号）
     */
    boolean cancelJob(String jobCode, String batchNo);
    
    /**
     * 重置作业状态（支持批次号）
     */
    boolean resetJobStatus(String jobCode, String batchNo);
    
    /**
     * 设置执行日志ID（支持批次号）
     */
    void setJobLogId(String jobCode, String batchNo, Long logId);
    
    /**
     * 获取执行日志ID（支持批次号）
     */
    long getJobLogId(String jobCode, String batchNo);
    
    /**
     * 等待作业暂停（支持批次号）
     */
    void waitForJobPause(String jobCode, String batchNo);
    
    // ==================== 状态查询（支持批次号） ====================
    
    /**
     * 获取作业组状态（支持批次号）
     */
    ExecutionStatus getGroupStatus(String groupName, String batchNo);
    
    /**
     * 获取单个作业状态（支持批次号）
     */
    ExecutionStatus getJobStatus(String jobCode, String batchNo);
    
    /**
     * 检查作业组是否暂停（支持批次号）
     */
    boolean isGroupPaused(String groupName, String batchNo);
    
    /**
     * 检查作业组是否停止（支持批次号）
     */
    boolean isGroupStopped(String groupName, String batchNo);
    
    /**
     * 检查作业组是否取消（支持批次号）
     */
    boolean isGroupCancelled(String groupName, String batchNo);
    
    /**
     * 检查作业是否暂停（支持批次号）
     */
    boolean isJobPaused(String jobCode, String batchNo);
    
    /**
     * 检查作业是否停止（支持批次号）
     */
    boolean isJobStopped(String jobCode, String batchNo);
    
    /**
     * 检查作业是否取消（支持批次号）
     */
    boolean isJobCancelled(String jobCode, String batchNo);
    
    // ==================== 批次级别操作 ====================
    
    /**
     * 加载作业组数据（支持批次号）
     */
    void loadJobGroupData(String groupName, String batchNo);
    
    /**
     * 获取作业组数据（支持批次号）
     */
    List<JobDef> getJobGroupData(String groupName, String batchNo);
    
    /**
     * 清理批次数据
     */
    void cleanupBatchData(String batchNo);
    
    /**
     * 获取批次中的所有作业代码
     */
    List<String> getJobCodesByBatch(String batchNo);
    
    /**
     * 获取批次中的所有作业组名称
     */
    List<String> getGroupNamesByBatch(String batchNo);
    
    /**
     * 从数据库获取批次统计信息
     */
    com.aia.gdp.dto.BatchStatistics getBatchStatisticsFromDatabase(String batchNo);
    
    // ==================== 根据执行日志ID操作 ====================
    
    /**
     * 根据执行日志ID暂停作业
     */
    boolean pauseJobByLogId(Long logId);
    
    /**
     * 根据执行日志ID恢复作业
     */
    boolean resumeJobByLogId(Long logId);
    
    /**
     * 根据执行日志ID停止作业
     */
    boolean stopJobByLogId(Long logId);
    
    /**
     * 根据执行日志ID取消作业
     */
    boolean cancelJobByLogId(Long logId);
    
    /**
     * 根据执行日志ID获取作业信息
     */
    JobExecutionLog getJobByLogId(Long logId);
    

    
    // ==================== 其他方法（保持原有签名） ====================
    
    /**
     * 取消批次中的所有作业组
     */
    boolean cancelAllJobGroups();
    
    /**
     * 取消指定批次号的所有作业
     */
    boolean cancelBatchJobs(String batchNo);
    
    /**
     * 获取作业最新执行状态（从执行日志获取）
     */
    ExecutionStatus getJobLatestExecutionStatus(String jobCode);
    
    /**
     * 获取所有作业组状态
     */
    Map<String, ExecutionStatus> getAllGroupStatus();
    
    /**
     * 获取所有作业状态
     */
    Map<String, ExecutionStatus> getAllJobStatus();
    
    /**
     * 获取作业组统计信息
     */
    GroupExecutionStatistics getGroupStatistics(String groupName);
    
    /**
     * 获取系统统计信息
     */
    SystemExecutionStatistics getSystemStatistics();
    
    /**
     * 检查是否可以执行作业组操作
     */
    boolean canExecuteGroupAction(String groupName,String batchNo,ControlAction action);
    boolean canExecuteGroupAction(String groupKey,ControlAction action);
    
    /**
     * 检查是否可以执行作业操作
     */
    boolean canExecuteJobAction(String jobCode, String batchNo,ControlAction action);
    boolean canExecuteJobAction(String jobKey,ControlAction action);
    
    /**
     * 初始化作业组（内部使用）
     */
    void initializeGroup(String groupName,String batchNo);
    
    /**
     * 初始化作业（内部使用）
     */
    void initializeJob(String jobCode,String batchNo);
    
    /**
     * 完成作业（内部使用）
     */
    void completeJob(String jobCode,String batchNo, boolean success);
    
    /**
     * 完成作业组（内部使用）
     */
    void completeGroup(String groupName,String batchNo, boolean success);
    
    /**
     * 等待作业组暂停
     */
    void waitForGroupPause(String groupName,String batchNo);
    

    
    /**
     * 清除作业组标志
     */
    void clearGroupFlags(String groupName,String batchNo);
    
    /**
     * 清除作业标志
     */
    void clearJobFlags(String jobCode,String batchNo);
    
    /**
     * 清除作业组取消状态
     */
    void clearGroupCancelledStatus(String groupName,String batchNo);
    
    /**
     * 清除作业取消状态
     */
    void clearJobCancelledStatus(String jobCode,String batchNo);
    
    /**
     * 重置作业组状态（用于重新开始执行）
     */
    void resetGroupStatus(String groupName,String batchNo);
    

    /**
     * 手动重置作业组取消状态（用于重新激活已取消的组）
     */
    void resetGroupCancelledStatus(String groupName,String batchNo);
    
    /**
     * 按需加载批次相关作业数据（外部触发）
     * 当需要处理某个批次时调用
     */
    void loadBatchJobData(String batchNo);
    
    /**
     * 按需加载单个作业数据（外部触发）
     * 当需要操作某个具体作业时调用
     */
    void loadJobData(String jobCode,String batchNo);
    
    /**
     * 按需加载多个作业数据（外部触发）
     * 当需要批量操作多个作业时调用
     */
    void loadMultipleJobData(List<String> jobCodes);
    
    /**
     * 预加载作业组数据（可选，用于性能优化）
     * 当预期某个作业组即将被使用时调用
     */
    void preloadJobGroupData(String groupName);
    
    /**
     * 清理不再需要的作业组数据（内存管理）
     * 当某个作业组长时间不使用时调用
     */
    void cleanupJobGroupData(String groupName);
    
    /**
     * 获取作业定义（优先从缓存获取）
     */
    JobDef getJobDef(String jobCode);
    
    /**
     * 获取所有活跃的作业组名称
     */
    Set<String> getActiveJobGroups();
    
    /**
     * 检查作业组是否存在
     */
    boolean isJobGroupExists(String groupKey);
    
    /**
     * 检查作业是否存在
     */
    boolean isJobExists(String jobKey);
    
    /**
     * 重置作业组统计信息
     */
    void resetGroupStatistics(String groupKey);
    
    
    // ==================== 统计信息类 ====================
    
    /**
     * 作业组执行统计信息
     */
    class GroupExecutionStatistics {
        private String groupName;
        private String batchNo;
        private int totalJobs;
        private int completedJobs;
        private int failedJobs;
        private int pausedJobs;
        private int stoppedJobs;
        private int cancelledJobs;
        private long startTime;
        private long endTime;
        private double progress;
        
        // Getters and Setters
        public String getGroupName() { return groupName; }
        public void setGroupName(String groupName) { this.groupName = groupName; }

        public String getBatchNo() { return batchNo; }
        public void setBatchNo(String batchNo) { this.batchNo = batchNo; }

        
        public int getTotalJobs() { return totalJobs; }
        public void setTotalJobs(int totalJobs) { this.totalJobs = totalJobs; }
        
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
        
        public long getStartTime() { return startTime; }
        public void setStartTime(long startTime) { this.startTime = startTime; }
        
        public long getEndTime() { return endTime; }
        public void setEndTime(long endTime) { this.endTime = endTime; }
        
        public double getProgress() { return progress; }
        public void setProgress(double progress) { this.progress = progress; }
        
        public long getDuration() {
            return endTime > 0 ? endTime - startTime : System.currentTimeMillis() - startTime;
        }
    }
    
    /**
     * 系统执行统计信息
     */
    class SystemExecutionStatistics {
        private int totalGroups;
        private int activeGroups;
        private int pausedGroups;
        private int stoppedGroups;
        private int cancelledGroups;
        private int totalJobs;
        private int activeJobs;
        private int pausedJobs;
        private int stoppedJobs;
        private int cancelledJobs;
        private long systemStartTime;
        private double overallProgress;
        
        // Getters and Setters
        public int getTotalGroups() { return totalGroups; }
        public void setTotalGroups(int totalGroups) { this.totalGroups = totalGroups; }
        
        public int getActiveGroups() { return activeGroups; }
        public void setActiveGroups(int activeGroups) { this.activeGroups = activeGroups; }
        
        public int getPausedGroups() { return pausedGroups; }
        public void setPausedGroups(int pausedGroups) { this.pausedGroups = pausedGroups; }
        
        public int getStoppedGroups() { return stoppedGroups; }
        public void setStoppedGroups(int stoppedGroups) { this.stoppedGroups = stoppedGroups; }
        
        public int getCancelledGroups() { return cancelledGroups; }
        public void setCancelledGroups(int cancelledGroups) { this.cancelledGroups = cancelledGroups; }
        
        public int getTotalJobs() { return totalJobs; }
        public void setTotalJobs(int totalJobs) { this.totalJobs = totalJobs; }
        
        public int getActiveJobs() { return activeJobs; }
        public void setActiveJobs(int activeJobs) { this.activeJobs = activeJobs; }
        
        public int getPausedJobs() { return pausedJobs; }
        public void setPausedJobs(int pausedJobs) { this.pausedJobs = pausedJobs; }
        
        public int getStoppedJobs() { return stoppedJobs; }
        public void setStoppedJobs(int stoppedJobs) { this.stoppedJobs = stoppedJobs; }
        
        public int getCancelledJobs() { return cancelledJobs; }
        public void setCancelledJobs(int cancelledJobs) { this.cancelledJobs = cancelledJobs; }
        
        public long getSystemStartTime() { return systemStartTime; }
        public void setSystemStartTime(long systemStartTime) { this.systemStartTime = systemStartTime; }
        
        public double getOverallProgress() { return overallProgress; }
        public void setOverallProgress(double overallProgress) { this.overallProgress = overallProgress; }
        
        public long getSystemUptime() {
            return System.currentTimeMillis() - systemStartTime;
        }
    }
} 