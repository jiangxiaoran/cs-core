package com.aia.gdp.controller;

import com.aia.gdp.common.ApiResponse;
import com.aia.gdp.dto.DashboardOverviewRequest;
import com.aia.gdp.dto.DashboardChartRequest;
import com.aia.gdp.service.DashboardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 仪表板控制器
 * 
 * @author andy
 * @date
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {
    
    private static final Logger logger = LoggerFactory.getLogger(DashboardController.class);
    
    @Autowired
    private DashboardService dashboardService;
    
    /**
     * 获取仪表板概览数据
     */
    @GetMapping("/overview")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getOverview(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String jobCode) {
        
        try {
            logger.info("获取仪表板概览数据请求: startDate={}, endDate={}, jobCode={}", startDate, endDate, jobCode);
            
            DashboardOverviewRequest request = new DashboardOverviewRequest(startDate, endDate, jobCode);
            Map<String, Object> result = dashboardService.getOverview(request);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取仪表板概览数据失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取仪表板概览数据失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取系统状态详情
     */
    @GetMapping("/system-status")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getSystemStatus() {
        try {
            logger.info("获取系统状态详情请求");
            
            Map<String, Object> result = dashboardService.getSystemStatus();
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取系统状态详情失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取系统状态详情失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取执行趋势图表数据
     */
    @GetMapping("/chart/execution-trend")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getExecutionTrend(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(defaultValue = "day") String groupBy,
            @RequestParam(required = false) String jobCode) {
        
        try {
            logger.info("获取执行趋势图表数据请求: startDate={}, endDate={}, groupBy={}, jobCode={}", 
                    startDate, endDate, groupBy, jobCode);
            
            DashboardChartRequest request = new DashboardChartRequest(startDate, endDate, groupBy, jobCode);
            Map<String, Object> result = dashboardService.getExecutionTrend(request);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取执行趋势图表数据失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取执行趋势图表数据失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取任务分组统计
     */
    @GetMapping("/chart/job-group-stats")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getJobGroupStats() {
        try {
            logger.info("获取任务分组统计请求");
            
            Map<String, Object> result = dashboardService.getJobGroupStats();
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取任务分组统计失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取任务分组统计失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取任务类型分布
     */
    @GetMapping("/chart/job-type-distribution")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getJobTypeDistribution() {
        try {
            logger.info("获取任务类型分布请求");
            
            Map<String, Object> result = dashboardService.getJobTypeDistribution();
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取任务类型分布失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取任务类型分布失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取执行器性能统计
     */
    @GetMapping("/chart/executor-performance")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getExecutorPerformance(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {
        
        try {
            logger.info("获取执行器性能统计请求: startDate={}, endDate={}", startDate, endDate);
            
            DashboardChartRequest request = new DashboardChartRequest(startDate, endDate, "day", null);
            Map<String, Object> result = dashboardService.getExecutorPerformance(request);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取执行器性能统计失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取执行器性能统计失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取最近执行记录
     */
    @GetMapping("/recent-executions")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getRecentExecutions(
            @RequestParam(defaultValue = "10") Integer limit) {
        
        try {
            logger.info("获取最近执行记录请求: limit={}", limit);
            
            Map<String, Object> result = dashboardService.getRecentExecutions(limit);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取最近执行记录失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取最近执行记录失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取当前运行任务
     */
    @GetMapping("/running-jobs")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getRunningJobs() {
        try {
            logger.info("获取当前运行任务请求");
            
            Map<String, Object> result = dashboardService.getRunningJobs();
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取当前运行任务失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取当前运行任务失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取系统告警信息
     */
    @GetMapping("/alerts")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getAlerts(
            @RequestParam(required = false) String level,
            @RequestParam(defaultValue = "10") Integer limit) {
        
        try {
            logger.info("获取系统告警信息请求: level={}, limit={}", level, limit);
            
            Map<String, Object> result = dashboardService.getAlerts(level, limit);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取系统告警信息失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取系统告警信息失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取日报表
     */
    @GetMapping("/report/daily")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getDailyReport(
            @RequestParam(required = false) String date) {
        
        try {
            logger.info("获取日报表请求: date={}", date);
            
            Map<String, Object> result = dashboardService.getDailyReport(date);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取日报表失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取日报表失败: " + e.getMessage()));
        }
    }
    
    /**
     * 获取月报表
     */
    @GetMapping("/report/monthly")
    public ResponseEntity<ApiResponse<Map<String, Object>>> getMonthlyReport(
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month) {
        
        try {
            logger.info("获取月报表请求: year={}, month={}", year, month);
            
            Map<String, Object> result = dashboardService.getMonthlyReport(year, month);
            
            return ResponseEntity.ok(ApiResponse.success("获取成功", result));
            
        } catch (Exception e) {
            logger.error("获取月报表失败", e);
            return ResponseEntity.ok(ApiResponse.error(500, "获取月报表失败: " + e.getMessage()));
        }
    }
} 