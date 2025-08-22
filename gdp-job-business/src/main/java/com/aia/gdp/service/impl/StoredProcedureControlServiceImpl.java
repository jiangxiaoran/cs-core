package com.aia.gdp.service.impl;

import com.aia.gdp.service.StoredProcedureControlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class StoredProcedureControlServiceImpl implements StoredProcedureControlService {
    
    private static final Logger logger = LoggerFactory.getLogger(StoredProcedureControlServiceImpl.class);
    
    @Autowired
    private StoredProcedureServiceImpl storedProcedureService;
    
    // 执行开始时间记录
    private final Map<String, Long> executionStartTimes = new ConcurrentHashMap<>();
    private final Map<String, String> executionProcedureNames = new ConcurrentHashMap<>();
    
    @Override
    public boolean pauseProcedure(String executionId, String reason) {
        try {
            StoredProcedureServiceImpl.ProcedureExecutionState state = 
                storedProcedureService.getExecutionState(executionId);
            
            if (state == null) {
                logger.warn("未找到执行状态: {}", executionId);
                return false;
            }
            
            if (state.isCancelled()) {
                logger.warn("执行已被取消，无法暂停: {}", executionId);
                return false;
            }
            
            if (state.isPaused()) {
                logger.warn("执行已经处于暂停状态: {}", executionId);
                return false;
            }
            
            storedProcedureService.pauseProcedure(executionId, reason);
            logger.info("存储过程执行已暂停: {} (执行ID: {}), 原因: {}", 
                       state.getCurrentStep(), executionId, reason);
            return true;
            
        } catch (Exception e) {
            logger.error("暂停存储过程执行失败: {}", executionId, e);
            return false;
        }
    }
    
    @Override
    public boolean resumeProcedure(String executionId) {
        try {
            StoredProcedureServiceImpl.ProcedureExecutionState state = 
                storedProcedureService.getExecutionState(executionId);
            
            if (state == null) {
                logger.warn("未找到执行状态: {}", executionId);
                return false;
            }
            
            if (!state.isPaused()) {
                logger.warn("执行未处于暂停状态: {}", executionId);
                return false;
            }
            
            if (state.isCancelled()) {
                logger.warn("执行已被取消，无法恢复: {}", executionId);
                return false;
            }
            
            storedProcedureService.resumeProcedure(executionId);
            logger.info("存储过程执行已恢复: {} (执行ID: {})", 
                       state.getCurrentStep(), executionId);
            return true;
            
        } catch (Exception e) {
            logger.error("恢复存储过程执行失败: {}", executionId, e);
            return false;
        }
    }
    
    @Override
    public boolean cancelProcedure(String executionId) {
        try {
            StoredProcedureServiceImpl.ProcedureExecutionState state = 
                storedProcedureService.getExecutionState(executionId);
            
            if (state == null) {
                logger.warn("未找到执行状态: {}", executionId);
                return false;
            }
            
            if (state.isCancelled()) {
                logger.warn("执行已经被取消: {}", executionId);
                return false;
            }
            
            storedProcedureService.cancelProcedure(executionId);
            logger.info("存储过程执行已取消: {} (执行ID: {})", 
                       state.getCurrentStep(), executionId);
            return true;
            
        } catch (Exception e) {
            logger.error("取消存储过程执行失败: {}", executionId, e);
            return false;
        }
    }
    
    @Override
    public StoredProcedureServiceImpl.ProcedureExecutionState getExecutionState(String executionId) {
        return storedProcedureService.getExecutionState(executionId);
    }
    
    @Override
    public Map<String, StoredProcedureServiceImpl.ProcedureExecutionState> getAllExecutionStates() {
        return storedProcedureService.getAllExecutionStates();
    }
    
    @Override
    public ExecutionStatistics getExecutionStatistics(String executionId) {
        StoredProcedureServiceImpl.ProcedureExecutionState state = 
            storedProcedureService.getExecutionState(executionId);
        
        if (state == null) {
            return null;
        }
        
        long startTime = executionStartTimes.getOrDefault(executionId, System.currentTimeMillis());
        String procedureName = executionProcedureNames.getOrDefault(executionId, "未知");
        long currentTime = System.currentTimeMillis();
        
        return new ExecutionStatistics(
            executionId,
            procedureName,
            startTime,
            currentTime,
            state.getTotalPauseTime(),
            state.getLastCheckpoint(),
            state.getProgress(),
            state.getCurrentStep(),
            state.isPaused(),
            state.isCancelled(),
            state.getPauseReason()
        );
    }
    
    @Override
    public boolean canPause(String executionId) {
        StoredProcedureServiceImpl.ProcedureExecutionState state = 
            storedProcedureService.getExecutionState(executionId);
        
        return state != null && !state.isPaused() && !state.isCancelled();
    }
    
    @Override
    public boolean canResume(String executionId) {
        StoredProcedureServiceImpl.ProcedureExecutionState state = 
            storedProcedureService.getExecutionState(executionId);
        
        return state != null && state.isPaused() && !state.isCancelled();
    }
    
    @Override
    public boolean canCancel(String executionId) {
        StoredProcedureServiceImpl.ProcedureExecutionState state = 
            storedProcedureService.getExecutionState(executionId);
        
        return state != null && !state.isCancelled();
    }
    
    /**
     * 记录执行开始时间
     */
    public void recordExecutionStart(String executionId, String procedureName) {
        executionStartTimes.put(executionId, System.currentTimeMillis());
        executionProcedureNames.put(executionId, procedureName);
        logger.info("记录执行开始: {} (执行ID: {})", procedureName, executionId);
    }
    
    /**
     * 清理执行记录
     */
    public void cleanupExecutionRecord(String executionId) {
        executionStartTimes.remove(executionId);
        executionProcedureNames.remove(executionId);
        logger.info("清理执行记录: {}", executionId);
    }
    
    /**
     * 获取所有执行统计信息
     */
    public Map<String, ExecutionStatistics> getAllExecutionStatistics() {
        Map<String, ExecutionStatistics> statistics = new HashMap<>();
        Map<String, StoredProcedureServiceImpl.ProcedureExecutionState> states = 
            storedProcedureService.getAllExecutionStates();
        
        for (Map.Entry<String, StoredProcedureServiceImpl.ProcedureExecutionState> entry : states.entrySet()) {
            String executionId = entry.getKey();
            ExecutionStatistics stat = getExecutionStatistics(executionId);
            if (stat != null) {
                statistics.put(executionId, stat);
            }
        }
        
        return statistics;
    }
} 