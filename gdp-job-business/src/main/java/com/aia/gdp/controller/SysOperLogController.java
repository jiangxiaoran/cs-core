package com.aia.gdp.controller;

import com.aia.gdp.common.ApiResponse;
import com.aia.gdp.model.SysOperLog;
import com.aia.gdp.service.SysOperLogService;
import com.aia.gdp.mapper.SysOperLogMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 系统操作日志控制器
 */
@RestController
@RequestMapping("/api/v1/operlog")
public class SysOperLogController {
    
    private static final Logger logger = LoggerFactory.getLogger(SysOperLogController.class);
    
    @Autowired
    private SysOperLogService sysOperLogService;
    
    @Autowired
    private SysOperLogMapper sysOperLogMapper;
    
    /**
     * 分页查询操作日志
     */
    @GetMapping("/list")
    public ResponseEntity<ApiResponse<IPage<SysOperLog>>> getOperLogList(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestParam(required = false) String title,
            @RequestParam(required = false) Integer businessType,
            @RequestParam(required = false) String operName,
            @RequestParam(required = false) Integer status,
            @RequestParam(required = false) String startTime,
            @RequestParam(required = false) String endTime) {
        
        try {
            Page<SysOperLog> page = new Page<>(current, size);
            IPage<SysOperLog> result = sysOperLogService.getOperLogPage(page, title, businessType, operName, status, startTime, endTime);
            
            return ResponseEntity.ok(ApiResponse.success("查询成功", result));
            
        } catch (Exception e) {
            logger.error("查询操作日志失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "查询操作日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 根据ID查询操作日志详情
     */
    @GetMapping("/{operId}")
    public ResponseEntity<ApiResponse<SysOperLog>> getOperLogById(@PathVariable Long operId) {
        try {
            SysOperLog operLog = sysOperLogService.getOperLogById(operId);
            
            if (operLog != null) {
                return ResponseEntity.ok(ApiResponse.success("查询成功", operLog));
            } else {
                return ResponseEntity.ok(ApiResponse.error(404, "操作日志不存在"));
            }
            
        } catch (Exception e) {
            logger.error("查询操作日志详情失败, operId: {}", operId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "查询操作日志详情失败: " + e.getMessage()));
        }
    }
    
    /**
     * 删除操作日志
     */
    @DeleteMapping("/{operId}")
    public ResponseEntity<ApiResponse<String>> deleteOperLog(@PathVariable Long operId) {
        try {
            boolean success = sysOperLogService.deleteOperLog(operId);
            
            if (success) {
                return ResponseEntity.ok(ApiResponse.success("删除成功", "操作日志已删除"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "删除失败"));
            }
            
        } catch (Exception e) {
            logger.error("删除操作日志失败, operId: {}", operId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "删除操作日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 清空操作日志
     */
    @DeleteMapping("/clear")
    public ResponseEntity<ApiResponse<String>> clearOperLog() {
        try {
            boolean success = sysOperLogService.clearOperLog();
            
            if (success) {
                return ResponseEntity.ok(ApiResponse.success("清空成功", "所有操作日志已删除"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "清空失败"));
            }
            
        } catch (Exception me) {
            logger.error("清空操作日志失败", me);
            return ResponseEntity.ok(ApiResponse.error(500, "清空操作日志失败: " + me.getMessage()));
        }
    }
    
    /**
     * 批量删除操作日志
     */
    @DeleteMapping("/batch-delete")
    public ResponseEntity<ApiResponse<String>> batchDeleteOperLog(@RequestBody Map<String, Object> request) {
        try {
            @SuppressWarnings("unchecked")
            java.util.List<Long> operIds = (java.util.List<Long>) request.get("operIds");
            
            if (operIds == null || operIds.isEmpty()) {
                return ResponseEntity.ok(ApiResponse.error(400, "请选择要删除的操作日志"));
            }
            
            boolean success = sysOperLogService.batchDeleteOperLog(operIds);
            
            if (success) {
                return ResponseEntity.ok(ApiResponse.success("批量删除成功", "选中的操作日志已删除"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "批量删除失败"));
            }
            
        } catch (Exception e) {
            logger.error("批量删除操作日志失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "批量删除操作日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取操作日志统计信息
     */
    @GetMapping("/statistics")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getOperLogStatistics() {
        try {
            Map<String, Object> statistics = new HashMap<>();
            
            // 获取总操作数
            long totalCount = sysOperLogMapper.selectCount(null);
            statistics.put("totalCount", totalCount);
            
            // 获取成功操作数（status = 0）
            long successCount = sysOperLogMapper.selectCount(new QueryWrapper<SysOperLog>().eq("status", 0));
            statistics.put("successCount", successCount);
            
            // 获取失败操作数（status = 1）
            long failureCount = sysOperLogMapper.selectCount(new QueryWrapper<SysOperLog>().eq("status", 1));
            statistics.put("failureCount", failureCount);
            
            // 获取今日操作数
            java.time.LocalDate today = java.time.LocalDate.now();
            java.time.LocalDateTime startOfDay = today.atStartOfDay();
            java.time.LocalDateTime endOfDay = today.atTime(23, 59, 59);
            
            long todayCount = sysOperLogMapper.selectCount(
                new QueryWrapper<SysOperLog>()
                    .ge("oper_time", startOfDay)
                    .le("oper_time", endOfDay)
            );
            statistics.put("todayCount", todayCount);
            
            return ResponseEntity.ok(ApiResponse.success("查询成功", statistics));
            
        } catch (Exception e) {
            logger.error("查询操作日志统计信息失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "查询操作日志统计信息失败: " + e.getMessage()));
        }
    }
}