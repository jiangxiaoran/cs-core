package com.aia.gdp.service;

import com.aia.gdp.dto.DashboardOverviewRequest;
import com.aia.gdp.dto.DashboardChartRequest;
import java.util.Map;

/**
 * 仪表板服务接口
 * 
 * @author andy
 * @date 2025-08-07
 */
public interface DashboardService {
    
    /**
     * 获取仪表板概览数据
     */
    Map<String, Object> getOverview(DashboardOverviewRequest request);
    
    /**
     * 获取系统状态详情
     */
    Map<String, Object> getSystemStatus();
    
    /**
     * 获取执行趋势图表数据
     */
    Map<String, Object> getExecutionTrend(DashboardChartRequest request);
    
    /**
     * 获取任务分组统计
     */
    Map<String, Object> getJobGroupStats();
    
    /**
     * 获取任务类型分布
     */
    Map<String, Object> getJobTypeDistribution();
    
    /**
     * 获取执行器性能统计
     */
    Map<String, Object> getExecutorPerformance(DashboardChartRequest request);
    
    /**
     * 获取最近执行记录
     */
    Map<String, Object> getRecentExecutions(Integer limit);
    
    /**
     * 获取当前运行任务
     */
    Map<String, Object> getRunningJobs();
    
    /**
     * 获取系统告警信息
     */
    Map<String, Object> getAlerts(String level, Integer limit);
    
    /**
     * 获取日报表
     */
    Map<String, Object> getDailyReport(String date);
    
    /**
     * 获取月报表
     */
    Map<String, Object> getMonthlyReport(Integer year, Integer month);
} 