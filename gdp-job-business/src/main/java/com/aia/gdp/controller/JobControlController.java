package com.aia.gdp.controller;

import com.aia.gdp.common.ApiResponse;
import com.aia.gdp.dto.BatchJobRequest;
import com.aia.gdp.dto.JobStartRequest;
import com.aia.gdp.service.JobControlService;
import com.aia.gdp.service.impl.JobControlServiceImpl;
import com.aia.gdp.annotation.OperLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * 作业控制REST控制器
 * 提供作业队列的暂停、恢复、终止、取消等控制API
 *
 * @author andy
 * @date 2025-08-02
 * @company
 */
@RestController
@RequestMapping("/api/v1/job-control")
public class JobControlController {
    
    private static final Logger logger = LoggerFactory.getLogger(JobControlController.class);
    
    @Autowired
    private JobControlService jobControlService;
    
    @Autowired
    private JobControlServiceImpl jobControlServiceImpl;
    
    // ==================== 新增任务控制功能 ====================
    
    /**
     * 启动任务
     */
    @OperLog(title = "启动任务", businessType = 1)
    @PostMapping("/jobs/{jobId}/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> startJob(
            @PathVariable Long jobId,
            @RequestBody(required = false) JobStartRequest request) {
        
        try {
            logger.info("启动任务请求: jobId={}", jobId);
            
            if (request == null) {
                request = new JobStartRequest();
            }
            
            Map<String, Object> result = jobControlServiceImpl.startJob(jobId, request);
            
            return ResponseEntity.ok(ApiResponse.success("启动成功", result));
            
        } catch (Exception e) {
            logger.error("启动任务失败, jobId: {}", jobId, e);
            return ResponseEntity.ok(ApiResponse.error(500, "启动任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 批量启动任务
     */
    @OperLog(title = "批量启动任务", businessType = 1)
    @PostMapping("/jobs/batch/start")
    public ResponseEntity<ApiResponse<Map<String, Object>>> batchStartJobs(@RequestBody BatchJobRequest request) {
        try {
            logger.info("批量启动任务请求: jobIds={}", request.getJobIds());
            
            Map<String, Object> result = jobControlServiceImpl.batchStartJobs(request);
            
            return ResponseEntity.ok(ApiResponse.success("批量启动成功", result));
            
        } catch (Exception e) {
            logger.error("批量启动任务失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "批量启动任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 批量停止任务
     */
    @OperLog(title = "批量停止任务", businessType = 2)
    @PostMapping("/jobs/batch/stop")
    public ResponseEntity<ApiResponse<Map<String, Object>>> batchStopJobs(@RequestBody BatchJobRequest request) {
        try {
            logger.info("批量停止任务请求: jobIds={}", request.getJobIds());
            
            Map<String, Object> result = jobControlServiceImpl.batchStopJobs(request);
            
            return ResponseEntity.ok(ApiResponse.success("批量停止成功", result));
            
        } catch (Exception e) {
            logger.error("批量停止任务失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "批量停止任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 根据批次号批量停止任务
     */
    @OperLog(title = "根据批次号批量停止任务", businessType = 2)
    @PostMapping("/batch/{batchNo}/stop")
    public ResponseEntity<ApiResponse<Map<String, Object>>> batchStopJobsByBatchNo(@PathVariable String batchNo) {
        try {
            logger.info("根据批次号批量停止任务请求: batchNo={}", batchNo);
            
            // 获取批次中的所有作业代码
            List<String> jobCodes = jobControlService.getJobCodesByBatch(batchNo);
            if (jobCodes.isEmpty()) {
                return ResponseEntity.ok(ApiResponse.error(404, "批次号 " + batchNo + " 中没有找到作业"));
            }
            
            // 批量停止
            int successCount = 0;
            int failCount = 0;
            List<String> failedJobs = new ArrayList<>();
            
            for (String jobCode : jobCodes) {
                try {
                    boolean success = jobControlService.stopJob(jobCode, batchNo);
                    if (success) {
                        successCount++;
                    } else {
                        failCount++;
                        failedJobs.add(jobCode);
                    }
                } catch (Exception e) {
                    failCount++;
                    failedJobs.add(jobCode);
                    logger.error("停止作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
                }
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("batchNo", batchNo);
            result.put("totalJobs", jobCodes.size());
            result.put("successCount", successCount);
            result.put("failCount", failCount);
            result.put("failedJobs", failedJobs);
            
            String message = String.format("批次 %s 批量停止完成: 成功 %d 个，失败 %d 个", 
                batchNo, successCount, failCount);
            
            return ResponseEntity.ok(ApiResponse.success(message, result));
            
        } catch (Exception e) {
            logger.error("根据批次号批量停止任务失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "根据批次号批量停止任务失败: " + e.getMessage()));
        }
    }
    
    // ==================== 现有作业组控制功能 ====================
    
    /**
     * 暂停作业组（支持批次号）
     */
    @OperLog(title = "暂停作业组", businessType = 2)
    @PostMapping("/group/{groupName}/pause")
    public ResponseEntity<Map<String, Object>> pauseJobGroup(
            @PathVariable String groupName,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到暂停作业组请求: groupName={}, batchNo={}", groupName, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.pauseJobGroup(groupName, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "pause");
            response.put("message", success ? "作业组暂停成功" : "作业组暂停失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("暂停作业组失败: groupName={}, batchNo={}", groupName, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "pause");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 恢复作业组（支持批次号）
     */
    @OperLog(title = "恢复作业组", businessType = 2)
    @PostMapping("/group/{groupName}/resume")
    public ResponseEntity<Map<String, Object>> resumeJobGroup(
            @PathVariable String groupName,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到恢复作业组请求: groupName={}, batchNo={}", groupName, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.resumeJobGroup(groupName, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "resume");
            response.put("message", success ? "作业组恢复成功" : "作业组恢复失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("恢复作业组失败: groupName={}, batchNo={}", groupName, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "resume");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 停止作业组（支持批次号）
     */
    @OperLog(title = "停止作业组", businessType = 2)
    @PostMapping("/group/{groupName}/stop")
    public ResponseEntity<Map<String, Object>> stopJobGroup(
            @PathVariable String groupName,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到停止作业组请求: groupName={}, batchNo={}", groupName, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.stopJobGroup(groupName, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "stop");
            response.put("message", success ? "作业组停止成功" : "作业组停止失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("停止作业组失败: groupName={}, batchNo={}", groupName, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "stop");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 取消作业组（支持批次号）
     */
    @OperLog(title = "取消作业组", businessType = 3)
    @PostMapping("/group/{groupName}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJobGroup(
            @PathVariable String groupName,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到取消作业组请求: groupName={}, batchNo={}", groupName, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.cancelJobGroup(groupName, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "cancel");
            response.put("message", success ? "作业组取消成功" : "作业组取消失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("取消作业组失败: groupName={}, batchNo={}", groupName, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("action", "cancel");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 取消所有作业组
     */
    @OperLog(title = "取消所有作业组", businessType = 3)
    @PostMapping("/group/cancel-all")
    public ResponseEntity<Map<String, Object>> cancelAllJobGroups() {
        try {
            logger.info("收到取消所有作业组请求");
            
            boolean success = jobControlService.cancelAllJobGroups();
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("action", "cancel-all");
            response.put("message", success ? "所有作业组取消成功" : "取消所有作业组失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("取消所有作业组失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("action", "cancel-all");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 取消指定批次的所有作业
     */
    @OperLog(title = "取消批次作业", businessType = 3)
    @PostMapping("/batch/{batchNo}/cancel")
    public ResponseEntity<Map<String, Object>> cancelBatchJobs(@PathVariable String batchNo) {
        try {
            logger.info("收到取消批次作业请求: {}", batchNo);
            
            boolean success = jobControlService.cancelBatchJobs(batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("batchNo", batchNo);
            response.put("action", "cancel-batch");
            response.put("message", success ? "批次作业取消成功" : "批次作业取消失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("取消批次作业失败: {}", batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("batchNo", batchNo);
            response.put("action", "cancel-batch");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 暂停单个作业（支持批次号）
     */
    @OperLog(title = "暂停作业", businessType = 2)
    @PostMapping("/job/{jobCode}/pause")
    public ResponseEntity<Map<String, Object>> pauseJob(
            @PathVariable String jobCode,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到暂停作业请求: jobCode={}, batchNo={}", jobCode, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.pauseJob(jobCode, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "pause");
            response.put("message", success ? "作业暂停成功" : "作业暂停失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("暂停作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "pause");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 根据执行日志ID暂停作业
     */
    @OperLog(title = "根据执行日志ID暂停作业", businessType = 2)
    @PostMapping("/job/execution/{logId}/pause")
    public ResponseEntity<Map<String, Object>> pauseJobByLogId(@PathVariable Long logId) {
        try {
            logger.info("收到根据执行日志ID暂停作业请求: logId={}", logId);
            
            // 这里需要从JobExecutionLog中获取jobCode和batchNo
            // 然后调用service方法
            boolean success = jobControlService.pauseJobByLogId(logId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("logId", logId);
            response.put("action", "pause");
            response.put("message", success ? "作业暂停成功" : "作业暂停失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("根据执行日志ID暂停作业失败: logId={}", logId, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("logId", logId);
            response.put("action", "pause");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 恢复单个作业（支持批次号）
     */
    @OperLog(title = "恢复作业", businessType = 2)
    @PostMapping("/job/{jobCode}/resume")
    public ResponseEntity<Map<String, Object>> resumeJob(
            @PathVariable String jobCode,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到恢复作业请求: jobCode={}, batchNo={}", jobCode, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.resumeJob(jobCode, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "resume");
            response.put("message", success ? "作业恢复成功" : "作业恢复失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("恢复作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "resume");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 根据执行日志ID恢复作业
     */
    @OperLog(title = "根据执行日志ID恢复作业", businessType = 2)
    @PostMapping("/job/execution/{logId}/resume")
    public ResponseEntity<Map<String, Object>> resumeJobByLogId(@PathVariable Long logId) {
        try {
            logger.info("收到根据执行日志ID恢复作业请求: logId={}", logId);
            
            boolean success = jobControlService.resumeJobByLogId(logId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("logId", logId);
            response.put("action", "resume");
            response.put("message", success ? "作业恢复成功" : "作业恢复失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("根据执行日志ID恢复作业失败: logId={}", logId, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("logId", logId);
            response.put("action", "resume");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 停止单个作业（支持批次号）
     */
    @OperLog(title = "停止作业", businessType = 2)
    @PostMapping("/job/{jobCode}/stop")
    public ResponseEntity<Map<String, Object>> stopJob(
            @PathVariable String jobCode,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到停止作业请求: jobCode={}, batchNo={}", jobCode, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.stopJob(jobCode, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "stop");
            response.put("message", success ? "作业停止成功" : "作业停止失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("停止作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "stop");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 根据执行日志ID停止作业
     */
    @OperLog(title = "根据执行日志ID停止作业", businessType = 2)
    @PostMapping("/job/execution/{logId}/stop")
    public ResponseEntity<Map<String, Object>> stopJobByLogId(@PathVariable Long logId) {
        try {
            logger.info("收到根据执行日志ID停止作业请求: logId={}", logId);
            
            boolean success = jobControlService.stopJobByLogId(logId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("logId", logId);
            response.put("action", "stop");
            response.put("message", success ? "作业停止成功" : "作业停止失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("根据执行日志ID停止作业失败: logId={}", logId, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("logId", logId);
            response.put("action", "stop");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 取消单个作业（支持批次号）
     */
    @OperLog(title = "取消作业", businessType = 3)
    @PostMapping("/job/{jobCode}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJob(
            @PathVariable String jobCode,
            @RequestParam(required = false) String batchNo) {
        try {
            logger.info("收到取消作业请求: jobCode={}, batchNo={}", jobCode, batchNo);
            
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            boolean success = jobControlService.cancelJob(jobCode, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "cancel");
            response.put("message", success ? "作业取消成功" : "作业取消失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("取消作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("action", "cancel");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 根据执行日志ID取消作业
     */
    @OperLog(title = "根据执行日志ID取消作业", businessType = 3)
    @PostMapping("/job/execution/{logId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelJobByLogId(@PathVariable Long logId) {
        try {
            logger.info("收到根据执行日志ID取消作业请求: logId={}", logId);
            
            boolean success = jobControlService.cancelJobByLogId(logId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", success);
            response.put("logId", logId);
            response.put("action", "cancel");
            response.put("message", success ? "作业取消成功" : "作业取消失败");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("根据执行日志ID取消作业失败: logId={}", logId, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("logId", logId);
            response.put("action", "cancel");
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取作业组状态（支持批次号）
     */
    @OperLog(title = "查询作业组状态", businessType = 0)
    @GetMapping("/group/{groupName}/status")
    public ResponseEntity<Map<String, Object>> getGroupStatus(
            @PathVariable String groupName,
            @RequestParam(required = false) String batchNo) {
        try {
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            JobControlService.ExecutionStatus status = jobControlService.getGroupStatus(groupName, batchNo);
            JobControlService.GroupExecutionStatistics statistics = jobControlService.getGroupStatistics(groupName);
            
            Map<String, Object> response = new HashMap<>();
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("status", status.name());
            response.put("statistics", statistics);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取作业组状态失败: groupName={}, batchNo={}", groupName, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("groupName", groupName);
            response.put("batchNo", batchNo);
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取单个作业状态（支持批次号）
     */
    @OperLog(title = "查询作业状态", businessType = 0)
    @GetMapping("/job/{jobCode}/status")
    public ResponseEntity<Map<String, Object>> getJobStatus(
            @PathVariable String jobCode,
            @RequestParam(required = false) String batchNo) {
        try {
            // 如果没有提供批次号，使用空字符串（向后兼容）
            if (batchNo == null) {
                batchNo = "";
            }
            
            JobControlService.ExecutionStatus status = jobControlService.getJobStatus(jobCode, batchNo);
            
            Map<String, Object> response = new HashMap<>();
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("status", status.name());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取作业状态失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("jobCode", jobCode);
            response.put("batchNo", batchNo);
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取所有作业组状态
     */
    @OperLog(title = "查询所有作业组状态", businessType = 0)
    @GetMapping("/groups/status")
    public ResponseEntity<Map<String, Object>> getAllGroupStatus() {
        try {
            Map<String, JobControlService.ExecutionStatus> groupStatus = jobControlService.getAllGroupStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("groupStatus", groupStatus);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取所有作业组状态失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取所有作业状态
     */
    @OperLog(title = "查询所有作业状态", businessType = 0)
    @GetMapping("/jobs/status")
    public ResponseEntity<Map<String, Object>> getAllJobStatus() {
        try {
            Map<String, JobControlService.ExecutionStatus> jobStatus = jobControlService.getAllJobStatus();
            
            Map<String, Object> response = new HashMap<>();
            response.put("jobStatus", jobStatus);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取所有作业状态失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 获取系统统计信息
     */
    @OperLog(title = "查询系统统计信息", businessType = 0)
    @GetMapping("/system/statistics")
    public ResponseEntity<Map<String, Object>> getSystemStatistics() {
        try {
            JobControlService.SystemExecutionStatistics statistics = jobControlService.getSystemStatistics();
            
            Map<String, Object> response = new HashMap<>();
            response.put("statistics", statistics);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("获取系统统计信息失败", e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 检查作业组操作权限
     */
    @OperLog(title = "检查作业组操作权限", businessType = 0)
    @GetMapping("/group/{groupName}/can-execute/{action}")
    public ResponseEntity<Map<String, Object>> canExecuteGroupAction(
            @PathVariable String groupName, 
            @PathVariable String action) {
        try {
            JobControlService.ControlAction controlAction = JobControlService.ControlAction.valueOf(action.toUpperCase());

            boolean canExecute = jobControlService.canExecuteGroupAction(groupName, controlAction);
            
            Map<String, Object> response = new HashMap<>();
            response.put("groupName", groupName);
            response.put("action", action);
            response.put("canExecute", canExecute);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("检查作业组操作权限失败: {} {}", groupName, action, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("groupName", groupName);
            response.put("action", action);
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    /**
     * 检查作业操作权限
     */
    @OperLog(title = "检查作业操作权限", businessType = 0)
    @GetMapping("/job/{jobCode}/can-execute/{action}")
    public ResponseEntity<Map<String, Object>> canExecuteJobAction(
            @PathVariable String jobCode, 
            @PathVariable String action) {
        try {
            JobControlService.ControlAction controlAction = JobControlService.ControlAction.valueOf(action.toUpperCase());
            boolean canExecute = jobControlService.canExecuteJobAction(jobCode, controlAction);
            
            Map<String, Object> response = new HashMap<>();
            response.put("jobCode", jobCode);
            response.put("action", action);
            response.put("canExecute", canExecute);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("检查作业操作权限失败: {} {}", jobCode, action, e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("jobCode", jobCode);
            response.put("action", action);
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }
    
    // ==================== 批次级别操作 ====================
    
    /**
     * 获取批次中的所有作业
     */
    @OperLog(title = "查询批次作业", businessType = 0)
    @GetMapping("/batch/{batchNo}/jobs")
    public ResponseEntity<ApiResponse<List<String>>> getBatchJobs(@PathVariable String batchNo) {
        try {
            logger.info("查询批次作业请求: batchNo={}", batchNo);
            
            List<String> jobCodes = jobControlService.getJobCodesByBatch(batchNo);
            
            return ResponseEntity.ok(ApiResponse.success("查询成功", jobCodes));
            
        } catch (Exception e) {
            logger.error("查询批次作业失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "查询批次作业失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取批次中的所有作业组
     */
    @OperLog(title = "查询批次作业组", businessType = 0)
    @GetMapping("/batch/{batchNo}/groups")
    public ResponseEntity<ApiResponse<List<String>>> getBatchGroups(@PathVariable String batchNo) {
        try {
            logger.info("查询批次作业组请求: batchNo={}", batchNo);
            
            List<String> groupNames = jobControlService.getGroupNamesByBatch(batchNo);
            
            return ResponseEntity.ok(ApiResponse.success("查询成功", groupNames));
            
        } catch (Exception e) {
            logger.error("查询批次作业组失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "查询批次作业组失败: " + e.getMessage()));
        }
    }
    
    /**
     * 清理批次数据
     */
    @OperLog(title = "清理批次数据", businessType = 3)
    @DeleteMapping("/batch/{batchNo}")
    public ResponseEntity<ApiResponse<String>> cleanupBatchData(@PathVariable String batchNo) {
        try {
            logger.info("清理批次数据请求: batchNo={}", batchNo);
            
            jobControlService.cleanupBatchData(batchNo);
            
            return ResponseEntity.ok(ApiResponse.success("批次数据清理成功", batchNo));
            
        } catch (Exception e) {
            logger.error("清理批次数据失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "清理批次数据失败: " + e.getMessage()));
        }
    }
    
    /**
     * 根据批次号批量暂停任务
     */
    @OperLog(title = "根据批次号批量暂停任务", businessType = 2)
    @PostMapping("/batch/{batchNo}/pause")
    public ResponseEntity<ApiResponse<Map<String, Object>>> batchPauseJobsByBatchNo(@PathVariable String batchNo) {
        try {
            logger.info("根据批次号批量暂停任务请求: batchNo={}", batchNo);
            
            // 获取批次中的所有作业代码
            List<String> jobCodes = jobControlService.getJobCodesByBatch(batchNo);
            if (jobCodes.isEmpty()) {
                return ResponseEntity.ok(ApiResponse.error(404, "批次号 " + batchNo + " 中没有找到作业"));
            }
            
            // 批量暂停
            int successCount = 0;
            int failCount = 0;
            List<String> failedJobs = new ArrayList<>();
            
            for (String jobCode : jobCodes) {
                try {
                    boolean success = jobControlService.pauseJob(jobCode, batchNo);
                    if (success) {
                        successCount++;
                    } else {
                        failCount++;
                        failedJobs.add(jobCode);
                    }
                } catch (Exception e) {
                    failCount++;
                    failedJobs.add(jobCode);
                    logger.error("暂停作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
                }
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("batchNo", batchNo);
            result.put("totalJobs", jobCodes.size());
            result.put("successCount", successCount);
            result.put("failCount", failCount);
            result.put("failedJobs", failedJobs);
            
            String message = String.format("批次 %s 批量暂停完成: 成功 %d 个，失败 %d 个", 
                batchNo, successCount, failCount);
            
            return ResponseEntity.ok(ApiResponse.success(message, result));
            
        } catch (Exception e) {
            logger.error("根据批次号批量暂停任务失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "根据批次号批量暂停任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 根据批次号批量恢复任务
     */
    @OperLog(title = "根据批次号批量恢复任务", businessType = 1)
    @PostMapping("/batch/{batchNo}/resume")
    public ResponseEntity<ApiResponse<Map<String, Object>>> batchResumeJobsByBatchNo(@PathVariable String batchNo) {
        try {
            logger.info("根据批次号批量恢复任务请求: batchNo={}", batchNo);
            
            // 获取批次中的所有作业代码
            List<String> jobCodes = jobControlService.getJobCodesByBatch(batchNo);
            if (jobCodes.isEmpty()) {
                return ResponseEntity.ok(ApiResponse.error(404, "批次号 " + batchNo + " 中没有找到作业"));
            }
            
            // 批量恢复
            int successCount = 0;
            int failCount = 0;
            List<String> failedJobs = new ArrayList<>();
            
            for (String jobCode : jobCodes) {
                try {
                    boolean success = jobControlService.resumeJob(jobCode, batchNo);
                    if (success) {
                        successCount++;
                    } else {
                        failCount++;
                        failedJobs.add(jobCode);
                    }
                } catch (Exception e) {
                    failCount++;
                    failedJobs.add(jobCode);
                    logger.error("恢复作业失败: jobCode={}, batchNo={}", jobCode, batchNo, e);
                }
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("batchNo", batchNo);
            result.put("totalJobs", jobCodes.size());
            result.put("successCount", successCount);
            result.put("failCount", failCount);
            result.put("failedJobs", failedJobs);
            
            String message = String.format("批次 %s 批量恢复完成: 成功 %d 个，失败 %d 个", 
                batchNo, successCount, failCount);
            
            return ResponseEntity.ok(ApiResponse.success(message, result));
            
        } catch (Exception e) {
            logger.error("根据批次号批量恢复任务失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "根据批次号批量恢复任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取批次统计信息
     */
    @OperLog(title = "查询批次统计", businessType = 0)
    @GetMapping("/batch/{batchNo}/statistics")
    public ResponseEntity<ApiResponse<com.aia.gdp.dto.BatchStatistics>> getBatchStatistics(@PathVariable String batchNo) {
        try {
            logger.info("查询批次统计请求: batchNo={}", batchNo);
            
            com.aia.gdp.dto.BatchStatistics statistics = jobControlService.getBatchStatisticsFromDatabase(batchNo);
            
            if (statistics != null) {
                return ResponseEntity.ok(ApiResponse.success("查询成功", statistics));
            } else {
                return ResponseEntity.ok(ApiResponse.error(404, "批次号 " + batchNo + " 未找到或统计失败"));
            }
            
        } catch (Exception e) {
            logger.error("查询批次统计失败: batchNo={}", batchNo, e);
            return ResponseEntity.ok(ApiResponse.error(500, "查询批次统计失败: " + e.getMessage()));
        }
    }
} 