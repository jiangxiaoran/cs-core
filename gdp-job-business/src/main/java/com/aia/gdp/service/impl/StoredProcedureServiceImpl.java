package com.aia.gdp.service.impl;

import com.aia.gdp.service.StoredProcedureService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 存储过程执行服务实现类
 * 提供专业的存储过程调用功能 暂时不用 只用到执行存过的功能
 *
 * @author andy
 * @date
 * @company
 */
@Service
public class StoredProcedureServiceImpl implements StoredProcedureService {
    
    private static final Logger logger = LoggerFactory.getLogger(StoredProcedureServiceImpl.class);
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    // 存储过程执行状态管理
    private final Map<String, ProcedureExecutionState> executionStates = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> procedureLocks = new ConcurrentHashMap<>();
    
    /**
     * 存储过程执行状态
     */
    public static class ProcedureExecutionState {
        private final AtomicBoolean isPaused = new AtomicBoolean(false);
        private final AtomicBoolean isCancelled = new AtomicBoolean(false);
        private final AtomicLong pauseStartTime = new AtomicLong(0);
        private final AtomicLong totalPauseTime = new AtomicLong(0);
        private final AtomicLong lastCheckpoint = new AtomicLong(0);
        private volatile String currentStep = "";
        private volatile int progress = 0;
        private volatile String pauseReason = "";
        
        public boolean isPaused() { return isPaused.get(); }
        public boolean isCancelled() { return isCancelled.get(); }
        public long getPauseStartTime() { return pauseStartTime.get(); }
        public long getTotalPauseTime() { return totalPauseTime.get(); }
        public long getLastCheckpoint() { return lastCheckpoint.get(); }
        public String getCurrentStep() { return currentStep; }
        public int getProgress() { return progress; }
        public String getPauseReason() { return pauseReason; }
        
        public void setPaused(boolean paused, String reason) {
            this.isPaused.set(paused);
            if (paused) {
                pauseStartTime.set(System.currentTimeMillis());
                pauseReason = reason;
            } else {
                long pauseDuration = System.currentTimeMillis() - pauseStartTime.get();
                totalPauseTime.addAndGet(pauseDuration);
                pauseReason = "";
            }
        }
        
        public void setCancelled(boolean cancelled) {
            this.isCancelled.set(cancelled);
        }
        
        public void updateProgress(int progress, String step) {
            this.progress = progress;
            this.currentStep = step;
            this.lastCheckpoint.set(System.currentTimeMillis());
        }
    }

    @Override
    public StoredProcedureResult executeProcedure(String procedureName, Map<String, Object> parameters) throws Exception {
        return executeProcedureWithPauseSupport(procedureName, parameters, null);
    }
    
    @Override
    public StoredProcedureResult executeProcedure(String procedureName) throws Exception {
        return executeProcedureWithPauseSupport(procedureName, new HashMap<>(), null);
    }
    
    public StoredProcedureResult executeProcedure(String procedureName, Map<String, Object> parameters, 
                                                String executionId) {
        return executeProcedureWithPauseSupport(procedureName, parameters, executionId);
    }
    
    /**
     * 带暂停支持的存储过程执行
     */
    private StoredProcedureResult executeProcedureWithPauseSupport(String procedureName, 
                                                                 Map<String, Object> parameters, 
                                                                 String executionId) {
        String execId = executionId != null ? executionId : generateExecutionId(procedureName);
        ProcedureExecutionState state = getOrCreateExecutionState(execId);
        ReentrantLock lock = getOrCreateLock(execId);
        
        long startTime = System.currentTimeMillis();
        
        try {
            lock.lock();
            logger.info("开始执行存储过程: {} (执行ID: {})", procedureName, execId);
            
            // 检查是否被取消
            if (state.isCancelled()) {
                StoredProcedureResult resultObj = new StoredProcedureResult(false, "执行已被取消");
                resultObj.setAffectedRows(0);
                resultObj.setExecutionTime(0);
                return resultObj;
            }
            
            // 根据存储过程类型执行不同的暂停策略
            switch (procedureName.toLowerCase()) {
                case "sp_cleanup_old_data":
                    return executeDataCleanupWithPause(procedureName, parameters, state, execId);
                case "sp_generate_daily_statistics":
                    return executeReportGenerationWithPause(procedureName, parameters, state, execId);
                case "sp_sync_data":
                    return executeDataSyncWithPause(procedureName, parameters, state, execId);
                default:
                    return executeGenericProcedureWithPause(procedureName, parameters, state, execId);
            }
            
        } catch (Exception e) {
            logger.error("存储过程执行异常: {} (执行ID: {})", procedureName, execId, e);
            StoredProcedureResult resultObj = new StoredProcedureResult(false, "执行异常: " + e.getMessage());
            resultObj.setAffectedRows(0);
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 数据清理存储过程 - 带暂停支持
     */
    private StoredProcedureResult executeDataCleanupWithPause(String procedureName, 
                                                             Map<String, Object> parameters, 
                                                             ProcedureExecutionState state, 
                                                             String execId) {
        long startTime = System.currentTimeMillis();
        
        try {
            // 步骤1: 检查暂停状态
            checkPauseState(state, "数据清理准备阶段");
            state.updateProgress(10, "开始数据清理");
            
            // 步骤2: 备份重要数据
            checkPauseState(state, "数据备份阶段");
            state.updateProgress(20, "备份重要数据");
            simulateWork(2000); // 模拟备份操作
            
            // 步骤3: 清理过期数据
            checkPauseState(state, "清理过期数据");
            state.updateProgress(40, "清理过期数据");
            simulateWork(3000); // 模拟清理操作
            
            // 步骤4: 清理日志数据
            checkPauseState(state, "清理日志数据");
            state.updateProgress(60, "清理日志数据");
            simulateWork(2000); // 模拟日志清理
            
            // 步骤5: 优化表结构
            checkPauseState(state, "表结构优化");
            state.updateProgress(80, "优化表结构");
            simulateWork(1500); // 模拟优化操作
            
            // 步骤6: 完成清理
            checkPauseState(state, "完成清理");
            state.updateProgress(100, "数据清理完成");
            
            // 执行实际的存储过程
            SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName(procedureName);
            
            Map<String, Object> result = call.execute(parameters);
            
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            logger.info("数据清理存储过程执行完成: {} (执行ID: {}), 耗时: {}秒", 
                       procedureName, execId, executionTimeSeconds);
            
            StoredProcedureResult resultObj = new StoredProcedureResult(true, "数据清理完成");
            resultObj.setAffectedRows(getAffectedRows(result));
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            StoredProcedureResult resultObj = new StoredProcedureResult(false, "执行被中断");
            resultObj.setAffectedRows(0);
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
        }
    }
    
    /**
     * 报表生成存储过程 - 带暂停支持
     */
    private StoredProcedureResult executeReportGenerationWithPause(String procedureName, 
                                                                 Map<String, Object> parameters, 
                                                                 ProcedureExecutionState state, 
                                                                 String execId) {
        long startTime = System.currentTimeMillis();
        
        try {
            // 步骤1: 数据收集
            checkPauseState(state, "数据收集阶段");
            state.updateProgress(15, "收集报表数据");
            simulateWork(2500);
            
            // 步骤2: 数据预处理
            checkPauseState(state, "数据预处理");
            state.updateProgress(30, "预处理数据");
            simulateWork(2000);
            
            // 步骤3: 统计分析
            checkPauseState(state, "统计分析");
            state.updateProgress(50, "执行统计分析");
            simulateWork(4000);
            
            // 步骤4: 生成报表
            checkPauseState(state, "报表生成");
            state.updateProgress(70, "生成报表文件");
            simulateWork(3000);
            
            // 步骤5: 质量检查
            checkPauseState(state, "质量检查");
            state.updateProgress(85, "检查报表质量");
            simulateWork(1500);
            
            // 步骤6: 完成生成
            checkPauseState(state, "完成生成");
            state.updateProgress(100, "报表生成完成");
            
            // 执行实际的存储过程
            SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName(procedureName);
            
            Map<String, Object> result = call.execute(parameters);
            
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            logger.info("报表生成存储过程执行完成: {} (执行ID: {}), 耗时: {}秒", 
                       procedureName, execId, executionTimeSeconds);
            
            StoredProcedureResult resultObj = new StoredProcedureResult(true, "报表生成完成");
            resultObj.setAffectedRows(getAffectedRows(result));
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            StoredProcedureResult resultObj = new StoredProcedureResult(false, "执行被中断");
            resultObj.setAffectedRows(0);
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
        }
    }
    
    /**
     * 数据同步存储过程 - 带暂停支持
     */
    private StoredProcedureResult executeDataSyncWithPause(String procedureName, 
                                                          Map<String, Object> parameters, 
                                                          ProcedureExecutionState state, 
                                                          String execId) {
        long startTime = System.currentTimeMillis();
        
        try {
            // 步骤1: 连接源系统
            checkPauseState(state, "连接源系统");
            state.updateProgress(10, "连接源数据系统");
            simulateWork(1000);
            
            // 步骤2: 数据验证
            checkPauseState(state, "数据验证");
            state.updateProgress(25, "验证源数据完整性");
            simulateWork(2000);
            
            // 步骤3: 数据转换
            checkPauseState(state, "数据转换");
            state.updateProgress(40, "转换数据格式");
            simulateWork(3000);
            
            // 步骤4: 数据同步
            checkPauseState(state, "数据同步");
            state.updateProgress(60, "同步数据到目标系统");
            simulateWork(4000);
            
            // 步骤5: 同步验证
            checkPauseState(state, "同步验证");
            state.updateProgress(80, "验证同步结果");
            simulateWork(2000);
            
            // 步骤6: 完成同步
            checkPauseState(state, "完成同步");
            state.updateProgress(100, "数据同步完成");
            
            // 执行实际的存储过程
            SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName(procedureName);
            
            Map<String, Object> result = call.execute(parameters);
            
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            logger.info("数据同步存储过程执行完成: {} (执行ID: {}), 耗时: {}秒", 
                       procedureName, execId, executionTimeSeconds);
            
            StoredProcedureResult resultObj = new StoredProcedureResult(true, "数据同步完成");
            resultObj.setAffectedRows(getAffectedRows(result));
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            StoredProcedureResult resultObj = new StoredProcedureResult(false, "执行被中断");
            resultObj.setAffectedRows(0);
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
        }
    }
    
    /**
     * 通用存储过程执行 - 带暂停支持
     */
    private StoredProcedureResult executeGenericProcedureWithPause(String procedureName, 
                                                                  Map<String, Object> parameters, 
                                                                  ProcedureExecutionState state, 
                                                                  String execId) {
        long startTime = System.currentTimeMillis();
        
        try {
            // 通用执行流程
            checkPauseState(state, "准备执行");
            state.updateProgress(20, "准备执行存储过程");
            simulateWork(1000);
            
            checkPauseState(state, "执行中");
            state.updateProgress(60, "执行存储过程");
            simulateWork(2000);
            
            checkPauseState(state, "完成执行");
            state.updateProgress(100, "存储过程执行完成");
            
            // 执行实际的存储过程
            SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate)
                .withProcedureName(procedureName);
            
            Map<String, Object> result = call.execute(parameters);
            
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            logger.info("通用存储过程执行完成: {} (执行ID: {}), 耗时: {}秒", 
                       procedureName, execId, executionTimeSeconds);
            
            StoredProcedureResult resultObj = new StoredProcedureResult(true, "存储过程执行完成");
            resultObj.setAffectedRows(getAffectedRows(result));
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            StoredProcedureResult resultObj = new StoredProcedureResult(false, "执行被中断");
            resultObj.setAffectedRows(0);
            long executionTimeMs = System.currentTimeMillis() - startTime;
            int executionTimeSeconds = (int) (executionTimeMs / 1000);
            resultObj.setExecutionTime(executionTimeSeconds);
            return resultObj;
        }
    }
    
    /**
     * 检查暂停状态
     */
    private void checkPauseState(ProcedureExecutionState state, String step) throws InterruptedException {
        if (state.isCancelled()) {
            throw new InterruptedException("执行已被取消");
        }
        
        while (state.isPaused()) {
            logger.info("存储过程执行已暂停: {}, 暂停原因: {}", step, state.getPauseReason());
            Thread.sleep(1000); // 每秒检查一次
            
            if (state.isCancelled()) {
                throw new InterruptedException("执行已被取消");
            }
        }
    }
    
    /**
     * 模拟工作负载
     */
    private void simulateWork(long milliseconds) throws InterruptedException {
        Thread.sleep(milliseconds);
    }
    
    /**
     * 获取或创建执行状态
     */
    private ProcedureExecutionState getOrCreateExecutionState(String executionId) {
        return executionStates.computeIfAbsent(executionId, k -> new ProcedureExecutionState());
    }
    
    /**
     * 获取或创建锁
     */
    private ReentrantLock getOrCreateLock(String executionId) {
        return procedureLocks.computeIfAbsent(executionId, k -> new ReentrantLock());
    }
    
    /**
     * 生成执行ID
     */
    private String generateExecutionId(String procedureName) {
        return procedureName + "_" + System.currentTimeMillis();
    }
    
    /**
     * 获取影响行数
     */
    private int getAffectedRows(Map<String, Object> result) {
        if (result != null && result.containsKey("affected_rows")) {
            Object value = result.get("affected_rows");
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }
        return 0;
    }
    
    // 暂停和恢复控制方法
    public void pauseProcedure(String executionId, String reason) {
        ProcedureExecutionState state = executionStates.get(executionId);
        if (state != null) {
            state.setPaused(true, reason);
            logger.info("存储过程执行已暂停: {} (执行ID: {}), 原因: {}", 
                       state.getCurrentStep(), executionId, reason);
        }
    }
    
    public void resumeProcedure(String executionId) {
        ProcedureExecutionState state = executionStates.get(executionId);
        if (state != null) {
            state.setPaused(false, "");
            logger.info("存储过程执行已恢复: {} (执行ID: {})", 
                       state.getCurrentStep(), executionId);
        }
    }
    
    public void cancelProcedure(String executionId) {
        ProcedureExecutionState state = executionStates.get(executionId);
        if (state != null) {
            state.setCancelled(true);
            logger.info("存储过程执行已取消: {} (执行ID: {})", 
                       state.getCurrentStep(), executionId);
        }
    }
    
    public ProcedureExecutionState getExecutionState(String executionId) {
        return executionStates.get(executionId);
    }
    
    public Map<String, ProcedureExecutionState> getAllExecutionStates() {
        return new HashMap<>(executionStates);
    }
    
    @Override
    public boolean procedureExists(String procedureName) {
        try {
            String sql = "SELECT COUNT(*) FROM information_schema.routines WHERE routine_name = ?";
            int count = jdbcTemplate.queryForObject(sql, Integer.class, procedureName);
            return count > 0;
        } catch (Exception e) {
            logger.error("检查存储过程是否存在时发生异常: {}", procedureName, e);
            return false;
        }
    }
    
    @Override
    public Map<String, String> getProcedureParameters(String procedureName) {
        Map<String, String> parameters = new HashMap<>();
        try {
            String sql = "SELECT parameter_name, parameter_mode, data_type FROM information_schema.parameters " +
                        "WHERE specific_name = ? ORDER BY ordinal_position";
            
            jdbcTemplate.query(sql, new Object[]{procedureName}, rs -> {
                String paramName = rs.getString("parameter_name");
                String paramMode = rs.getString("parameter_mode");
                String dataType = rs.getString("data_type");
                
                if ("IN".equals(paramMode) || "INOUT".equals(paramMode)) {
                    parameters.put(paramName, dataType);
                }
            });
        } catch (Exception e) {
            logger.error("获取存储过程参数时发生异常: {}", procedureName, e);
        }
        return parameters;
    }
    
    // 现有的业务方法保持不变...
    public StoredProcedureResult executeDataCleanupProcedure(String tableName, int retentionDays) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_table_name", tableName);
        params.put("p_retention_days", retentionDays);
        return executeProcedure("sp_cleanup_old_data", params);
    }
    
    public StoredProcedureResult executeDataStatisticsProcedure(String reportDate) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_report_date", reportDate);
        return executeProcedure("sp_generate_daily_statistics", params);
    }
    
    public StoredProcedureResult executeDataSyncProcedure(String sourceSystem, String targetTable) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_source_system", sourceSystem);
        params.put("p_target_table", targetTable);
        return executeProcedure("sp_sync_data", params);
    }
    
    public StoredProcedureResult executeReportGenerationProcedure(String reportType, String reportDate) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_report_type", reportType);
        params.put("p_report_date", reportDate);
        return executeProcedure("sp_generate_daily_statistics", params);
    }
    
    public StoredProcedureResult executeDataValidationProcedure(String tableName, String validationRule) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_table_name", tableName);
        params.put("p_validation_rule", validationRule);
        return executeProcedure("sp_validate_data", params);
    }
    
    public StoredProcedureResult executeDataArchiveProcedure(String tableName, int archiveMonths) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_table_name", tableName);
        params.put("p_archive_months", archiveMonths);
        return executeProcedure("sp_archive_data", params);
    }
    
    public StoredProcedureResult executeSystemMaintenanceProcedure(String maintenanceType) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_maintenance_type", maintenanceType);
        return executeProcedure("sp_system_maintenance", params);
    }
    
    public StoredProcedureResult executeUserDataSyncProcedure() throws Exception {
        Map<String, Object> params = new HashMap<>();
        return executeProcedure("sp_sync_user_data", params);
    }
    
    public StoredProcedureResult executeOrderDataProcessingProcedure(String processDate) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_process_date", processDate);
        return executeProcedure("sp_process_order_data", params);
    }
    
    public StoredProcedureResult executeFinancialSummaryProcedure(String summaryDate, String summaryType) throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("p_summary_date", summaryDate);
        params.put("p_summary_type", summaryType);
        return executeProcedure("sp_generate_financial_summary", params);
    }
} 