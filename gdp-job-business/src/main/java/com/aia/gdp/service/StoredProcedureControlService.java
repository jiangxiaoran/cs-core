package com.aia.gdp.service;

import com.aia.gdp.service.impl.StoredProcedureServiceImpl.ProcedureExecutionState;
import java.util.Map;

/**
 * 存储过程控制服务接口
 * 提供存储过程执行的控制功能
 */
public interface StoredProcedureControlService {
    
    /**
     * 暂停存储过程执行
     * @param executionId 执行ID
     * @param reason 暂停原因
     * @return 是否成功暂停
     */
    boolean pauseProcedure(String executionId, String reason);
    
    /**
     * 恢复存储过程执行
     * @param executionId 执行ID
     * @return 是否成功恢复
     */
    boolean resumeProcedure(String executionId);
    
    /**
     * 取消存储过程执行
     * @param executionId 执行ID
     * @return 是否成功取消
     */
    boolean cancelProcedure(String executionId);
    
    /**
     * 获取执行状态
     * @param executionId 执行ID
     * @return 执行状态
     */
    ProcedureExecutionState getExecutionState(String executionId);
    
    /**
     * 获取所有执行状态
     * @return 所有执行状态
     */
    Map<String, ProcedureExecutionState> getAllExecutionStates();
    
    /**
     * 获取执行统计信息
     * @param executionId 执行ID
     * @return 统计信息
     */
    ExecutionStatistics getExecutionStatistics(String executionId);
    
    /**
     * 检查执行是否可暂停
     * @param executionId 执行ID
     * @return 是否可暂停
     */
    boolean canPause(String executionId);
    
    /**
     * 检查执行是否可恢复
     * @param executionId 执行ID
     * @return 是否可恢复
     */
    boolean canResume(String executionId);
    
    /**
     * 检查执行是否可取消
     * @param executionId 执行ID
     * @return 是否可取消
     */
    boolean canCancel(String executionId);
    
    /**
     * 执行统计信息
     */
    class ExecutionStatistics {
        private final String executionId;
        private final String procedureName;
        private final long startTime;
        private final long currentTime;
        private final long totalPauseTime;
        private final long lastCheckpoint;
        private final int progress;
        private final String currentStep;
        private final boolean isPaused;
        private final boolean isCancelled;
        private final String pauseReason;
        
        public ExecutionStatistics(String executionId, String procedureName, 
                                 long startTime, long currentTime, long totalPauseTime,
                                 long lastCheckpoint, int progress, String currentStep,
                                 boolean isPaused, boolean isCancelled, String pauseReason) {
            this.executionId = executionId;
            this.procedureName = procedureName;
            this.startTime = startTime;
            this.currentTime = currentTime;
            this.totalPauseTime = totalPauseTime;
            this.lastCheckpoint = lastCheckpoint;
            this.progress = progress;
            this.currentStep = currentStep;
            this.isPaused = isPaused;
            this.isCancelled = isCancelled;
            this.pauseReason = pauseReason;
        }
        
        public String getExecutionId() { return executionId; }
        public String getProcedureName() { return procedureName; }
        public long getStartTime() { return startTime; }
        public long getCurrentTime() { return currentTime; }
        public long getTotalPauseTime() { return totalPauseTime; }
        public long getLastCheckpoint() { return lastCheckpoint; }
        public int getProgress() { return progress; }
        public String getCurrentStep() { return currentStep; }
        public boolean isPaused() { return isPaused; }
        public boolean isCancelled() { return isCancelled; }
        public String getPauseReason() { return pauseReason; }
        
        public long getElapsedTime() {
            return currentTime - startTime - totalPauseTime;
        }
        
        public double getProgressPercentage() {
            return progress;
        }
        
        public String getStatus() {
            if (isCancelled) return "已取消";
            if (isPaused) return "已暂停";
            return "执行中";
        }
    }
} 