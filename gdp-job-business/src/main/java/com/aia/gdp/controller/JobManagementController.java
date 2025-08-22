package com.aia.gdp.controller;

import com.aia.gdp.common.ApiResponse;
import com.aia.gdp.dto.JobListRequest;
import com.aia.gdp.dto.JobLogListRequest;
import com.aia.gdp.dto.BatchLogDeleteRequest;
import com.aia.gdp.dto.LogCleanupRequest;
import com.aia.gdp.dto.LogExportRequest;
import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.service.JobManagementService;
import com.aia.gdp.service.JobExecutionLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

/**
 * 任务管理REST控制器
 * 提供任务的CRUD操作功能和执行日志管理功能
 *
 * @author andy
 * @date 2025-08-07
 */
@RestController
@RequestMapping("/api/v1/jobs")
public class JobManagementController {
    
    private static final Logger logger = LoggerFactory.getLogger(JobManagementController.class);
    
    @Autowired
    private JobManagementService jobManagementService;
    
    @Autowired
    private JobExecutionLogService jobExecutionLogService;
    
    // ==================== 任务管理功能 ====================
    
    /**
     * 获取任务列表
     */
    @GetMapping
    public ResponseEntity<ApiResponse<Map<String, Object>>> getJobList(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "10") Integer pageSize,
            @RequestParam(required = false) String jobCode,
            @RequestParam(required = false) String jobName,
            @RequestParam(required = false) String jobGroup,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String jobType,
            @RequestParam(required = false) Boolean isActive) {
        
        try {
            logger.info("获取任务列表请求: current={}, pageSize={}, jobCode={}, jobName={}, jobGroup={}, status={}, jobType={}, isActive={}",
                    current, pageSize, jobCode, jobName, jobGroup, status, jobType, isActive);
            
            JobListRequest request = new JobListRequest();
            request.setCurrent(current);
            request.setPageSize(pageSize);
            request.setJobCode(jobCode);
            request.setJobName(jobName);
            request.setJobGroup(jobGroup);
            request.setStatus(status);
            request.setJobType(jobType);
            request.setIsActive(isActive);
            
            Map<String, Object> result = jobManagementService.getJobList(request);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取任务列表失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取任务列表失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取任务详情
     */
    @GetMapping("/{jobId}")
    public ResponseEntity<ApiResponse<JobDef>> getJobDetail(@PathVariable Long jobId) {
        try {
            logger.info("获取任务详情请求: jobId={}", jobId);
            
            JobDef jobDef = jobManagementService.getJobDetail(jobId);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", jobDef));
            
        } catch (Exception e) {
            logger.error("获取任务详情失败, jobId: {}", jobId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取任务详情失败: " + e.getMessage()));
        }
    }
    
    /**
     * 创建任务
     */
    @PostMapping
    public ResponseEntity<ApiResponse<Map<String, Object>>> createJob(@RequestBody JobDef jobDef) {
        try {
            logger.info("创建任务请求: jobCode={}, jobName={}", jobDef.getJobCode(), jobDef.getJobName());
            
            Long jobId = jobManagementService.createJob(jobDef);
            
            Map<String, Object> result = new HashMap<>();
            result.put("jobId", jobId);
            
            return ResponseEntity.ok(ApiResponse.success("创建成功", result));
            
        } catch (Exception e) {
            logger.error("创建任务失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "创建任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 更新任务
     */
    @PutMapping("/{jobId}")
    public ResponseEntity<ApiResponse<Object>> updateJob(@PathVariable Long jobId, @RequestBody JobDef jobDef) {
        try {
            logger.info("更新任务请求: jobId={}", jobId);
            
            jobDef.setJobId(jobId);
            boolean result = jobManagementService.updateJob(jobId, jobDef);
            
            if (result) {
                return ResponseEntity.ok(ApiResponse.success("更新成功"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "更新失败"));
            }
            
        } catch (Exception e) {
            logger.error("更新任务失败, jobId: {}", jobId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "更新任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 删除任务
     */
    @DeleteMapping("/{jobId}")
    public ResponseEntity<ApiResponse<Object>> deleteJob(@PathVariable Long jobId) {
        try {
            logger.info("删除任务请求: jobId={}", jobId);
            
            boolean result = jobManagementService.deleteJob(jobId);
            
            if (result) {
                return ResponseEntity.ok(ApiResponse.success("删除成功"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "删除失败"));
            }
            
        } catch (Exception e) {
            logger.error("删除任务失败, jobId: {}", jobId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "删除任务失败: " + e.getMessage()));
        }
    }
    
    // ==================== 执行日志管理功能 ====================
    
    /**
     * 获取执行日志列表
     */
    @GetMapping("/logs")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getJobLogList(
            @RequestParam(defaultValue = "1") Integer current,
            @RequestParam(defaultValue = "10") Integer pageSize,
            @RequestParam(required = false) String jobCode,
            @RequestParam(required = false) String batchNo,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startTime,
            @RequestParam(required = false) String endTime,
            @RequestParam(required = false) String executorAddress) {
        
        try {
            logger.info("获取执行日志列表请求: current={}, pageSize={}, jobCode={}, batchNo={}, status={}",
                    current, pageSize, jobCode, batchNo, status);
            
            JobLogListRequest request = new JobLogListRequest();
            request.setCurrent(current);
            request.setPageSize(pageSize);
            request.setJobCode(jobCode);
            request.setBatchNo(batchNo);
            request.setStatus(status);
            request.setExecutorAddress(executorAddress);
            
            // 处理时间参数
            if (startTime != null) {
                try {
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    request.setStartTime(sdf.parse(startTime));
                } catch (Exception e) {
                    logger.warn("开始时间格式错误: {}", startTime);
                }
            }
            
            if (endTime != null) {
                try {
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    request.setEndTime(sdf.parse(endTime));
                } catch (Exception e) {
                    logger.warn("结束时间格式错误: {}", endTime);
                }
            }
            
            Map<String, Object> result = jobExecutionLogService.getJobLogList(request);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取执行日志列表失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取执行日志列表失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取执行日志详情
     */
    @GetMapping("/logs/{logId}")
    public ResponseEntity<ApiResponse<JobExecutionLog>> getJobLogDetail(@PathVariable Long logId) {
        try {
            logger.info("获取执行日志详情请求: logId={}", logId);
            
            JobExecutionLog log = jobExecutionLogService.getJobLogDetail(logId);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", log));
            
        } catch (Exception e) {
            logger.error("获取执行日志详情失败, logId: {}", logId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取执行日志详情失败: " + e.getMessage()));
        }
    }
    
    /**
     * 删除执行日志
     */
    @DeleteMapping("/logs/{logId}")
    public ResponseEntity<ApiResponse<Object>> deleteJobLog(@PathVariable Long logId) {
        try {
            logger.info("删除执行日志请求: logId={}", logId);
            
            boolean result = jobExecutionLogService.deleteJobLog(logId);
            
            if (result) {
                return ResponseEntity.ok(ApiResponse.success("删除成功"));
            } else {
                return ResponseEntity.ok(ApiResponse.error(500, "删除失败"));
            }
            
        } catch (Exception e) {
            logger.error("删除执行日志失败, logId: {}", logId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "删除执行日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 批量删除执行日志
     */
    @DeleteMapping("/logs/batch")
    public ResponseEntity<ApiResponse<Map<String, Object>>> batchDeleteJobLogs(@RequestBody BatchLogDeleteRequest request) {
        try {
            logger.info("批量删除执行日志请求: logIds={}", request.getLogIds());
            
            Map<String, Object> result = jobExecutionLogService.batchDeleteJobLogs(request);
            
            return ResponseEntity.ok(ApiResponse.success("批量删除成功", result));
            
        } catch (Exception e) {
            logger.error("批量删除执行日志失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "批量删除执行日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 清理历史日志
     */
    @PostMapping("/logs/cleanup")
    public ResponseEntity<ApiResponse<Map<String, Object>>> cleanupJobLogs(@RequestBody LogCleanupRequest request) {
        try {
            logger.info("清理历史日志请求: beforeDate={}, status={}, jobCodes={}",
                    request.getBeforeDate(), request.getStatus(), request.getJobCodes());
            
            Map<String, Object> result = jobExecutionLogService.cleanupJobLogs(request);
            
            return ResponseEntity.ok(ApiResponse.success("清理成功", result));
            
        } catch (Exception e) {
            logger.error("清理历史日志失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "清理历史日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取日志统计信息
     */
    @GetMapping("/logs/stats")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getJobLogStats(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String jobCode) {
        
        try {
            logger.info("获取日志统计信息请求: startDate={}, endDate={}, jobCode={}", startDate, endDate, jobCode);
            
            Map<String, Object> result = jobExecutionLogService.getJobLogStats(startDate, endDate, jobCode);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取日志统计信息失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取日志统计信息失败: " + e.getMessage()));
        }
    }
    
    /**
     * 导出执行日志
     */
    @PostMapping("/logs/export")
    public ResponseEntity<ApiResponse<Map<String, Object>>> exportJobLogs(@RequestBody LogExportRequest request) {
        try {
            logger.info("导出执行日志请求: startDate={}, endDate={}, format={}",
                    request.getStartDate(), request.getEndDate(), request.getFormat());
            
            Map<String, Object> result = jobExecutionLogService.exportJobLogs(request);
            
            return ResponseEntity.ok(ApiResponse.success("导出成功", result));
            
        } catch (Exception e) {
            logger.error("导出执行日志失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "导出执行日志失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取实时执行状态
     */
    @GetMapping("/logs/realtime")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getRealtimeJobLogs(
            @RequestParam(required = false) String jobCode,
            @RequestParam(required = false) String batchNo) {
        
        try {
            logger.info("获取实时执行状态请求: jobCode={}, batchNo={}", jobCode, batchNo);
            
            Map<String, Object> result = jobExecutionLogService.getRealtimeJobLogs(jobCode, batchNo);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取实时执行状态失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取实时执行状态失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取执行趋势分析
     */
    @GetMapping("/logs/analysis/trend")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getExecutionTrend(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(defaultValue = "day") String groupBy,
            @RequestParam(required = false) String jobCode) {
        
        try {
            logger.info("获取执行趋势分析请求: startDate={}, endDate={}, groupBy={}, jobCode={}",
                    startDate, endDate, groupBy, jobCode);
            
            Map<String, Object> result = jobExecutionLogService.getExecutionTrend(startDate, endDate, groupBy, jobCode);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取执行趋势分析失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取执行趋势分析失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取任务性能分析
     */
    @GetMapping("/logs/analysis/performance")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getJobPerformance(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String jobCode) {
        
        try {
            logger.info("获取任务性能分析请求: startDate={}, endDate={}, jobCode={}",
                    startDate, endDate, jobCode);
            
            Map<String, Object> result = jobExecutionLogService.getJobPerformance(startDate, endDate, jobCode);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取任务性能分析失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取任务性能分析失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取执行器列表
     */
    @GetMapping("/system/executors")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getExecutors(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String address) {
        
        try {
            logger.info("获取执行器列表请求: status={}, address={}", status, address);
            
            // 模拟执行器数据
            Map<String, Object> result = new HashMap<>();
            List<Map<String, Object>> executors = new ArrayList<>();
            
            // 执行器1
            Map<String, Object> executor1 = new HashMap<>();
            executor1.put("executorId", 1);
            executor1.put("appName", "gdp-job-executor");
            executor1.put("title", "GDP作业执行器");
            executor1.put("addressType", 0);
            executor1.put("addressList", "192.168.1.100:8081");
            executor1.put("registryTime", "2025-08-01T00:00:00Z");
            executor1.put("updateTime", "2025-08-30T10:00:00Z");
            executor1.put("status", "online");
            executor1.put("runningJobs", 5);
            executor1.put("cpuUsage", 30.0);
            executor1.put("memoryUsage", 50.0);
            executor1.put("lastHeartbeat", "2025-08-30T10:00:00Z");
            executors.add(executor1);
            
            // 执行器2
            Map<String, Object> executor2 = new HashMap<>();
            executor2.put("executorId", 2);
            executor2.put("appName", "gdp-job-executor-2");
            executor2.put("title", "GDP作业执行器2");
            executor2.put("addressType", 0);
            executor2.put("addressList", "192.168.1.101:8081");
            executor2.put("registryTime", "2025-08-01T00:00:00Z");
            executor2.put("updateTime", "2025-08-30T10:00:00Z");
            executor2.put("status", "online");
            executor2.put("runningJobs", 3);
            executor2.put("cpuUsage", 25.0);
            executor2.put("memoryUsage", 45.0);
            executor2.put("lastHeartbeat", "2025-08-30T10:00:00Z");
            executors.add(executor2);
            
            result.put("list", executors);
            result.put("total", executors.size());
            result.put("onlineCount", executors.size());
            result.put("offlineCount", 0);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取执行器列表失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取执行器列表失败: " + e.getMessage()));
        }
    }
} 