package com.aia.gdp.event;

import java.time.LocalDateTime;

/**
 * 作业控制事件基类
 * 用于事件驱动架构，解耦组件间的依赖
 * 第一个版本没有解耦导致循环依赖，虽然也能解决但不够完美。现在完全解耦
 *
 * @author andy
 * @date 2025-07-29
 * @company
 */
public abstract class JobControlEvent {
    
    private final String eventId;
    private final LocalDateTime timestamp;
    private final String source;
    
    protected JobControlEvent(String source) {
        this.eventId = java.util.UUID.randomUUID().toString();
        this.timestamp = LocalDateTime.now();
        this.source = source;
    }
    
    public String getEventId() {
        return eventId;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public String getSource() {
        return source;
    }
    
    @Override
    public String toString() {
        return String.format("%s{eventId='%s', timestamp=%s, source='%s'}", 
                           getClass().getSimpleName(), eventId, timestamp, source);
    }
    
    /**
     * 作业组暂停事件
     */
    public static class GroupPausedEvent extends JobControlEvent {
        private final String groupName;
        
        public GroupPausedEvent(String groupName) {
            super("JobControlService");
            this.groupName = groupName;
        }
        
        public String getGroupName() {
            return groupName;
        }
    }
    
    /**
     * 作业组恢复事件
     */
    public static class GroupResumedEvent extends JobControlEvent {
        private final String groupName;
        
        public GroupResumedEvent(String groupName) {
            super("JobControlService");
            this.groupName = groupName;
        }
        
        public String getGroupName() {
            return groupName;
        }
    }
    
    /**
     * 作业组停止事件
     */
    public static class GroupStoppedEvent extends JobControlEvent {
        private final String groupName;
        private final String reason;
        
        public GroupStoppedEvent(String groupName, String reason) {
            super("JobControlService");
            this.groupName = groupName;
            this.reason = reason;
        }
        
        public String getGroupName() {
            return groupName;
        }
        
        public String getReason() {
            return reason;
        }
    }
    
    /**
     * 作业组取消事件
     */
    public static class GroupCancelledEvent extends JobControlEvent {
        private final String groupName;
        private final String strategy;
        
        public GroupCancelledEvent(String groupName, String strategy) {
            super("JobControlService");
            this.groupName = groupName;
            this.strategy = strategy;
        }
        
        public String getGroupName() {
            return groupName;
        }
        
        public String getStrategy() {
            return strategy;
        }
    }
    
    /**
     * 作业组取消完成事件
     */
    public static class GroupCancelledCompletedEvent extends JobControlEvent {
        private final String groupName;
        
        public GroupCancelledCompletedEvent(String groupName) {
            super("JobControlService");
            this.groupName = groupName;
        }
        
        public String getGroupName() {
            return groupName;
        }
    }
    
    /**
     * 作业暂停事件
     */
    public static class JobPausedEvent extends JobControlEvent {
        private final String jobCode;
        
        public JobPausedEvent(String jobCode) {
            super("JobControlService");
            this.jobCode = jobCode;
        }
        
        public String getJobCode() {
            return jobCode;
        }
    }
    
    /**
     * 作业恢复事件
     */
    public static class JobResumedEvent extends JobControlEvent {
        private final String jobCode;
        
        public JobResumedEvent(String jobCode) {
            super("JobControlService");
            this.jobCode = jobCode;
        }
        
        public String getJobCode() {
            return jobCode;
        }
    }
    
    /**
     * 作业停止事件
     */
    public static class JobStoppedEvent extends JobControlEvent {
        private final String jobCode;
        private final String reason;
        
        public JobStoppedEvent(String jobCode, String reason) {
            super("JobControlService");
            this.jobCode = jobCode;
            this.reason = reason;
        }
        
        public String getJobCode() {
            return jobCode;
        }
        
        public String getReason() {
            return reason;
        }
    }
    
    /**
     * 作业取消事件
     */
    public static class JobCancelledEvent extends JobControlEvent {
        private final String jobCode;
        private final String strategy;
        
        public JobCancelledEvent(String jobCode, String strategy) {
            super("JobControlService");
            this.jobCode = jobCode;
            this.strategy = strategy;
        }
        
        public String getJobCode() {
            return jobCode;
        }
        
        public String getStrategy() {
            return strategy;
        }
    }
    
    /**
     * 作业启动事件
     */
    public static class JobStartedEvent extends JobControlEvent {
        private final String jobCode;
        private final String batchNo;
        
        public JobStartedEvent(String jobCode, String batchNo) {
            super("JobControlService");
            this.jobCode = jobCode;
            this.batchNo = batchNo;
        }
        
        public String getJobCode() {
            return jobCode;
        }
        
        public String getBatchNo() {
            return batchNo;
        }
    }
    
    /**
     * 作业执行注册事件
     */
    public static class JobExecutionRegisteredEvent extends JobControlEvent {
        private final String jobCode;
        private final String batchNo;
        private final String groupName;
        
        public JobExecutionRegisteredEvent(String jobCode, String batchNo, String groupName) {
            super("JobControlService");
            this.jobCode = jobCode;
            this.batchNo = batchNo;
            this.groupName = groupName;
        }
        
        public String getJobCode() {
            return jobCode;
        }
        
        public String getBatchNo() {
            return batchNo;
        }
        
        public String getGroupName() {
            return groupName;
        }
    }
    
    /**
     * 作业执行完成事件
     */
    public static class JobExecutionCompletedEvent extends JobControlEvent {
        private final String jobCode;
        private final String batchNo;
        
        public JobExecutionCompletedEvent(String jobCode, String batchNo) {
            super("JobControlService");
            this.jobCode = jobCode;
            this.batchNo = batchNo;
        }
        
        public String getJobCode() {
            return jobCode;
        }
        public String getBatchNo() {
            return batchNo;
        }
    }
    
    /**
     * 作业状态查询事件
     */
    public static class JobStatusQueryEvent extends JobControlEvent {
        private final String jobCode;
        private final String queryType; // "pause" 或 "stop"
        
        public JobStatusQueryEvent(String jobCode, String queryType) {
            super("WorkerHandler");
            this.jobCode = jobCode;
            this.queryType = queryType;
        }
        
        public String getJobCode() {
            return jobCode;
        }
        
        public String getQueryType() {
            return queryType;
        }
    }
} 