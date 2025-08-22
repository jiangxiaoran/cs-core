package com.aia.gdp.service;

import com.aia.gdp.service.impl.StoredProcedureServiceImpl.ProcedureExecutionState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 存储过程测试服务
 * 用于测试存储过程暂停功能
 */
@Service
public class StoredProcedureTestService {
    
    private static final Logger logger = LoggerFactory.getLogger(StoredProcedureTestService.class);
    
    @Autowired
    private StoredProcedureService storedProcedureService;
    
    @Autowired
    private StoredProcedureControlService controlService;
    
    /**
     * 测试基本存储过程执行
     */
    public void testBasicExecution() {
        try {
            logger.info("=== 开始测试基本存储过程执行 ===");
            
            // 测试数据清理存储过程
            Map<String, Object> params = new HashMap<>();
            params.put("p_table_name", "test_logs");
            params.put("p_retention_days", 30);
            
            logger.info("执行数据清理存储过程");
            StoredProcedureService.StoredProcedureResult result = 
                storedProcedureService.executeProcedure("sp_cleanup_old_data", params);
            
            logger.info("执行结果: {}", result);
            
        } catch (Exception e) {
            logger.error("测试基本执行失败", e);
        }
    }
    
    /**
     * 测试暂停和恢复功能
     */
    public void testPauseAndResume() {
        try {
            logger.info("=== 开始测试暂停和恢复功能 ===");
            
            String executionId = "test_pause_" + System.currentTimeMillis();
            
            // 异步执行存储过程
            CompletableFuture<StoredProcedureService.StoredProcedureResult> future = 
                CompletableFuture.supplyAsync(() -> {
                    try {
                        Map<String, Object> params = new HashMap<>();
                        params.put("p_table_name", "test_data");
                        params.put("p_retention_days", 30);
                        
                        return storedProcedureService.executeProcedure("sp_cleanup_old_data", params);
                    } catch (Exception e) {
                        logger.error("异步执行失败", e);
                        return null;
                    }
                });
            
            // 等待一段时间后暂停
            Thread.sleep(3000);
            logger.info("暂停执行: {}", executionId);
            boolean paused = controlService.pauseProcedure(executionId, "测试暂停");
            logger.info("暂停结果: {}", paused);
            
            // 检查暂停状态
            ProcedureExecutionState state = controlService.getExecutionState(executionId);
            if (state != null) {
                logger.info("执行状态: 暂停={}, 进度={}%, 当前步骤={}", 
                           state.isPaused(), state.getProgress(), state.getCurrentStep());
            }
            
            // 等待一段时间后恢复
            Thread.sleep(2000);
            logger.info("恢复执行: {}", executionId);
            boolean resumed = controlService.resumeProcedure(executionId);
            logger.info("恢复结果: {}", resumed);
            
            // 等待执行完成
            StoredProcedureService.StoredProcedureResult result = future.get(30, TimeUnit.SECONDS);
            logger.info("最终执行结果: {}", result);
            
        } catch (Exception e) {
            logger.error("测试暂停和恢复失败", e);
        }
    }
    
    /**
     * 测试取消功能
     */
    public void testCancel() {
        try {
            logger.info("=== 开始测试取消功能 ===");
            
            String executionId = "test_cancel_" + System.currentTimeMillis();
            
            // 异步执行存储过程
            CompletableFuture<StoredProcedureService.StoredProcedureResult> future = 
                CompletableFuture.supplyAsync(() -> {
                    try {
                        Map<String, Object> params = new HashMap<>();
                        params.put("p_report_type", "daily");
                        params.put("p_report_date", "2024-01-01");
                        
                        return storedProcedureService.executeProcedure("sp_generate_daily_statistics", params);
                    } catch (Exception e) {
                        logger.error("异步执行失败", e);
                        return null;
                    }
                });
            
            // 等待一段时间后取消
            Thread.sleep(2000);
            logger.info("取消执行: {}", executionId);
            boolean cancelled = controlService.cancelProcedure(executionId);
            logger.info("取消结果: {}", cancelled);
            
            // 等待执行完成
            try {
                StoredProcedureService.StoredProcedureResult result = future.get(10, TimeUnit.SECONDS);
                logger.info("取消后执行结果: {}", result);
            } catch (Exception e) {
                logger.info("执行被成功取消");
            }
            
        } catch (Exception e) {
            logger.error("测试取消功能失败", e);
        }
    }
    
    /**
     * 测试多步骤暂停
     */
    public void testMultiStepPause() {
        try {
            logger.info("=== 开始测试多步骤暂停 ===");
            
            String executionId = "test_multistep_" + System.currentTimeMillis();
            
            // 异步执行数据同步存储过程
            CompletableFuture<StoredProcedureService.StoredProcedureResult> future = 
                CompletableFuture.supplyAsync(() -> {
                    try {
                        Map<String, Object> params = new HashMap<>();
                        params.put("p_source_system", "external_db");
                        params.put("p_target_table", "user_data");
                        
                        return storedProcedureService.executeProcedure("sp_sync_data", params);
                    } catch (Exception e) {
                        logger.error("异步执行失败", e);
                        return null;
                    }
                });
            
            // 监控执行进度
            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                
                ProcedureExecutionState state = controlService.getExecutionState(executionId);
                if (state != null) {
                    logger.info("执行进度: {}%, 当前步骤: {}, 暂停状态: {}", 
                               state.getProgress(), state.getCurrentStep(), state.isPaused());
                    
                    // 在特定进度点暂停
                    if (state.getProgress() >= 40 && !state.isPaused()) {
                        logger.info("在40%进度点暂停执行");
                        controlService.pauseProcedure(executionId, "在数据转换阶段暂停");
                        break;
                    }
                }
            }
            
            // 等待一段时间后恢复
            Thread.sleep(3000);
            logger.info("恢复执行");
            controlService.resumeProcedure(executionId);
            
            // 等待执行完成
            StoredProcedureService.StoredProcedureResult result = future.get(30, TimeUnit.SECONDS);
            logger.info("多步骤暂停测试结果: {}", result);
            
        } catch (Exception e) {
            logger.error("测试多步骤暂停失败", e);
        }
    }
    
    /**
     * 测试执行统计
     */
    public void testExecutionStatistics() {
        try {
            logger.info("=== 开始测试执行统计 ===");
            
            String executionId = "test_stats_" + System.currentTimeMillis();
            
            // 执行存储过程
            Map<String, Object> params = new HashMap<>();
            params.put("p_table_name", "test_table");
            params.put("p_validation_rule", "basic");
            
            StoredProcedureService.StoredProcedureResult result = 
                storedProcedureService.executeProcedure("sp_validate_data", params);
            
            logger.info("执行结果: {}", result);
            
            // 获取执行统计
            StoredProcedureControlService.ExecutionStatistics statistics = 
                controlService.getExecutionStatistics(executionId);
            
            if (statistics != null) {
                logger.info("执行统计:");
                logger.info("  执行ID: {}", statistics.getExecutionId());
                logger.info("  存储过程: {}", statistics.getProcedureName());
                logger.info("  状态: {}", statistics.getStatus());
                logger.info("  进度: {}%", statistics.getProgressPercentage());
                logger.info("  执行时间: {}ms", statistics.getElapsedTime());
                logger.info("  暂停时间: {}ms", statistics.getTotalPauseTime());
                logger.info("  当前步骤: {}", statistics.getCurrentStep());
            }
            
        } catch (Exception e) {
            logger.error("测试执行统计失败", e);
        }
    }
    
    /**
     * 运行所有测试
     */
    public void runAllTests() {
        logger.info("开始运行存储过程暂停功能测试");
        
        try {
            // 测试基本执行
            testBasicExecution();
            Thread.sleep(2000);
            
            // 测试暂停和恢复
            testPauseAndResume();
            Thread.sleep(2000);
            
            // 测试取消
            testCancel();
            Thread.sleep(2000);
            
            // 测试多步骤暂停
            testMultiStepPause();
            Thread.sleep(2000);
            
            // 测试执行统计
            testExecutionStatistics();
            
            logger.info("所有测试完成");
            
        } catch (Exception e) {
            logger.error("运行测试失败", e);
        }
    }
} 