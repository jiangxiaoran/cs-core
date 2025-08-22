package com.aia.gdp.controller;

import com.aia.gdp.service.StoredProcedureControlService;
import com.aia.gdp.service.StoredProcedureService;
import com.aia.gdp.service.impl.StoredProcedureServiceImpl.ProcedureExecutionState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * 存储过程控制REST控制器
 * 提供存储过程执行的控制API (这个只预留接口暂时不实现功能，实现需要过程配合)
 *
 * @author andy
 * @date 2025-08-01
 * @company
 */
@RestController
@RequestMapping("/api/stored-procedure")
public class StoredProcedureController {
    
    private static final Logger logger = LoggerFactory.getLogger(StoredProcedureController.class);
    
    @Autowired
    private StoredProcedureControlService controlService;
    
    @Autowired
    private StoredProcedureService storedProcedureService;
    
    /**
     * 暂停存储过程执行
     */
    @PostMapping("/{executionId}/pause")
    public ResponseEntity<Map<String, Object>> pauseProcedure(
            @PathVariable String executionId,
            @RequestBody(required = false) PauseRequest request) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            String reason = request != null ? request.getReason() : "用户手动暂停";
            boolean success = controlService.pauseProcedure(executionId, reason);
            
            response.put("success", success);
            response.put("executionId", executionId);
            response.put("message", success ? "存储过程执行已暂停" : "暂停失败");
            
            if (success) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.badRequest().body(response);
            }
            
        } catch (Exception e) {
            logger.error("暂停存储过程执行异常: {}", executionId, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 恢复存储过程执行
     */
    @PostMapping("/{executionId}/resume")
    public ResponseEntity<Map<String, Object>> resumeProcedure(@PathVariable String executionId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            boolean success = controlService.resumeProcedure(executionId);
            
            response.put("success", success);
            response.put("executionId", executionId);
            response.put("message", success ? "存储过程执行已恢复" : "恢复失败");
            
            if (success) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.badRequest().body(response);
            }
            
        } catch (Exception e) {
            logger.error("恢复存储过程执行异常: {}", executionId, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 取消存储过程执行
     */
    @PostMapping("/{executionId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelProcedure(@PathVariable String executionId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            boolean success = controlService.cancelProcedure(executionId);
            
            response.put("success", success);
            response.put("executionId", executionId);
            response.put("message", success ? "存储过程执行已取消" : "取消失败");
            
            if (success) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.badRequest().body(response);
            }
            
        } catch (Exception e) {
            logger.error("取消存储过程执行异常: {}", executionId, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取执行状态
     */
    @GetMapping("/{executionId}/status")
    public ResponseEntity<Map<String, Object>> getExecutionStatus(@PathVariable String executionId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            ProcedureExecutionState state = controlService.getExecutionState(executionId);
            
            if (state == null) {
                response.put("success", false);
                response.put("message", "未找到执行状态");
                return ResponseEntity.notFound().build();
            }
            
            response.put("success", true);
            response.put("executionId", executionId);
            response.put("isPaused", state.isPaused());
            response.put("isCancelled", state.isCancelled());
            response.put("progress", state.getProgress());
            response.put("currentStep", state.getCurrentStep());
            response.put("pauseReason", state.getPauseReason());
            response.put("lastCheckpoint", state.getLastCheckpoint());
            response.put("totalPauseTime", state.getTotalPauseTime());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取执行状态异常: {}", executionId, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取执行统计信息
     */
    @GetMapping("/{executionId}/statistics")
    public ResponseEntity<Map<String, Object>> getExecutionStatistics(@PathVariable String executionId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            StoredProcedureControlService.ExecutionStatistics statistics = 
                controlService.getExecutionStatistics(executionId);
            
            if (statistics == null) {
                response.put("success", false);
                response.put("message", "未找到执行统计信息");
                return ResponseEntity.notFound().build();
            }
            
            response.put("success", true);
            response.put("executionId", statistics.getExecutionId());
            response.put("procedureName", statistics.getProcedureName());
            response.put("startTime", statistics.getStartTime());
            response.put("currentTime", statistics.getCurrentTime());
            response.put("elapsedTime", statistics.getElapsedTime());
            response.put("totalPauseTime", statistics.getTotalPauseTime());
            response.put("progress", statistics.getProgress());
            response.put("progressPercentage", statistics.getProgressPercentage());
            response.put("currentStep", statistics.getCurrentStep());
            response.put("status", statistics.getStatus());
            response.put("isPaused", statistics.isPaused());
            response.put("isCancelled", statistics.isCancelled());
            response.put("pauseReason", statistics.getPauseReason());
            response.put("lastCheckpoint", statistics.getLastCheckpoint());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取执行统计信息异常: {}", executionId, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取所有执行状态
     */
    @GetMapping("/status/all")
    public ResponseEntity<Map<String, Object>> getAllExecutionStatus() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            Map<String, ProcedureExecutionState> states = controlService.getAllExecutionStates();
            List<Map<String, Object>> statusList = new ArrayList<>();
            
            for (Map.Entry<String, ProcedureExecutionState> entry : states.entrySet()) {
                String executionId = entry.getKey();
                ProcedureExecutionState state = entry.getValue();
                
                Map<String, Object> status = new HashMap<>();
                status.put("executionId", executionId);
                status.put("isPaused", state.isPaused());
                status.put("isCancelled", state.isCancelled());
                status.put("progress", state.getProgress());
                status.put("currentStep", state.getCurrentStep());
                status.put("pauseReason", state.getPauseReason());
                
                statusList.add(status);
            }
            
            response.put("success", true);
            response.put("totalCount", states.size());
            response.put("executions", statusList);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取所有执行状态异常", e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 检查操作权限
     */
    @GetMapping("/{executionId}/permissions")
    public ResponseEntity<Map<String, Object>> getPermissions(@PathVariable String executionId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            response.put("success", true);
            response.put("executionId", executionId);
            response.put("canPause", controlService.canPause(executionId));
            response.put("canResume", controlService.canResume(executionId));
            response.put("canCancel", controlService.canCancel(executionId));
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取操作权限异常: {}", executionId, e);
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 暂停请求
     */
    public static class PauseRequest {
        private String reason;
        
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }
} 