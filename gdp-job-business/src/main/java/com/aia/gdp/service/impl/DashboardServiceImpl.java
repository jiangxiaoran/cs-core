package com.aia.gdp.service.impl;

import com.aia.gdp.dto.DashboardOverviewRequest;
import com.aia.gdp.dto.DashboardChartRequest;
import com.aia.gdp.mapper.JobDefMapper;
import com.aia.gdp.mapper.JobExecutionLogMapper;
import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.service.DashboardService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 仪表板服务实现类
 * 
 * @author andy
 * @date 2025-08-07
 */
@Service
public class DashboardServiceImpl implements DashboardService {
    
    private static final Logger logger = LoggerFactory.getLogger(DashboardServiceImpl.class);
    
    @Autowired
    private JobDefMapper jobDefMapper;
    
    @Autowired
    private JobExecutionLogMapper jobExecutionLogMapper;
    
    @Override
    public Map<String, Object> getOverview(DashboardOverviewRequest request) {
        try {
            Map<String, Object> result = new HashMap<>();
            
            // 获取任务统计
            QueryWrapper<JobDef> jobQuery = new QueryWrapper<>();
            if (StringUtils.hasText(request.getJobCode())) {
                jobQuery.like("job_code", request.getJobCode());
            }
            List<JobDef> allJobs = jobDefMapper.selectList(jobQuery);
            
            int totalJobs = allJobs.size();
            int activeJobs = (int) allJobs.stream().filter(job -> "active".equals(job.getStatus())).count();
            int runningJobs = (int) allJobs.stream().filter(job -> "running".equals(job.getStatus())).count();
            int failedJobs = (int) allJobs.stream().filter(job -> "failed".equals(job.getStatus())).count();
            
            // 获取执行统计
            QueryWrapper<JobExecutionLog> logQuery = new QueryWrapper<>();
            if (StringUtils.hasText(request.getStartDate())) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date start = sdf.parse(request.getStartDate());
                    logQuery.ge("start_time", start);
                } catch (Exception e) {
                    logger.warn("开始日期格式错误: {}", request.getStartDate());
                }
            }
            if (StringUtils.hasText(request.getEndDate())) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date end = sdf.parse(request.getEndDate());
                    logQuery.le("start_time", end);
                } catch (Exception e) {
                    logger.warn("结束日期格式错误: {}", request.getEndDate());
                }
            }
            if (StringUtils.hasText(request.getJobCode())) {
                logQuery.eq("job_code", request.getJobCode());
            }
            
            List<JobExecutionLog> allLogs = jobExecutionLogMapper.selectList(logQuery);
            
            int totalExecutions = allLogs.size();
            int successExecutions = (int) allLogs.stream().filter(log -> "success".equals(log.getStatus())).count();
            int failedExecutions = (int) allLogs.stream().filter(log -> "failed".equals(log.getStatus())).count();
            
            // 获取今日执行数
            QueryWrapper<JobExecutionLog> todayQuery = new QueryWrapper<>();
            todayQuery.ge("start_time", getStartOfDay());
            if (StringUtils.hasText(request.getJobCode())) {
                todayQuery.eq("job_code", request.getJobCode());
            }
            List<JobExecutionLog> todayLogs = jobExecutionLogMapper.selectList(todayQuery);
            int todayExecutions = todayLogs.size();
            
            // 系统状态（模拟数据）
            String systemStatus = "healthy";
            String lastUpdateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
            String systemUptime = "29天10小时30分钟";
            double cpuUsage = 25.5;
            double memoryUsage = 60.2;
            double diskUsage = 45.8;
            
            result.put("totalJobs", totalJobs);
            result.put("activeJobs", activeJobs);
            result.put("runningJobs", runningJobs);
            result.put("failedJobs", failedJobs);
            result.put("totalExecutions", totalExecutions);
            result.put("successExecutions", successExecutions);
            result.put("failedExecutions", failedExecutions);
            result.put("todayExecutions", todayExecutions);
            result.put("systemStatus", systemStatus);
            result.put("lastUpdateTime", lastUpdateTime);
            result.put("systemUptime", systemUptime);
            result.put("cpuUsage", cpuUsage);
            result.put("memoryUsage", memoryUsage);
            result.put("diskUsage", diskUsage);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取仪表板概览数据失败", e);
            throw new RuntimeException("获取仪表板概览数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getSystemStatus() {
        try {
            Map<String, Object> result = new HashMap<>();
            
            // 系统信息
            Map<String, Object> systemInfo = new HashMap<>();
            systemInfo.put("systemName", "GDP作业调度系统");
            systemInfo.put("version", "1.0.0");
            systemInfo.put("startTime", "2025-08-01T00:00:00Z");
            systemInfo.put("uptime", "29天10小时30分钟");
            systemInfo.put("lastHealthCheck", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
            
            // 资源使用情况
            Map<String, Object> resourceUsage = new HashMap<>();
            resourceUsage.put("cpuUsage", 25.5);
            resourceUsage.put("memoryUsage", 60.2);
            resourceUsage.put("diskUsage", 45.8);
            resourceUsage.put("networkIn", "1.2MB/s");
            resourceUsage.put("networkOut", "0.8MB/s");
            
            // 执行器状态（模拟数据）
            List<Map<String, Object>> executorStatus = new ArrayList<>();
            Map<String, Object> executor1 = new HashMap<>();
            executor1.put("executorAddress", "192.168.1.100:8081");
            executor1.put("status", "healthy");
            executor1.put("lastHeartbeat", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
            executor1.put("runningJobs", 5);
            executor1.put("cpuUsage", 30.0);
            executor1.put("memoryUsage", 50.0);
            executorStatus.add(executor1);
            
            Map<String, Object> executor2 = new HashMap<>();
            executor2.put("executorAddress", "192.168.1.101:8081");
            executor2.put("status", "healthy");
            executor2.put("lastHeartbeat", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
            executor2.put("runningJobs", 7);
            executor2.put("cpuUsage", 40.0);
            executor2.put("memoryUsage", 65.0);
            executorStatus.add(executor2);
            
            // 数据库状态
            Map<String, Object> databaseStatus = new HashMap<>();
            databaseStatus.put("status", "connected");
            databaseStatus.put("connectionCount", 15);
            databaseStatus.put("slowQueries", 2);
            databaseStatus.put("lastBackup", "2025-08-29T02:00:00Z");
            
            result.put("systemInfo", systemInfo);
            result.put("resourceUsage", resourceUsage);
            result.put("executorStatus", executorStatus);
            result.put("databaseStatus", databaseStatus);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取系统状态详情失败", e);
            throw new RuntimeException("获取系统状态详情失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getExecutionTrend(DashboardChartRequest request) {
        try {
            logger.info("开始获取执行趋势图表数据: startDate={}, endDate={}, groupBy={}, jobCode={}", 
                    request.getStartDate(), request.getEndDate(), request.getGroupBy(), request.getJobCode());
            
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            // 修复日期范围查询逻辑
            if (StringUtils.hasText(request.getStartDate())) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date start = sdf.parse(request.getStartDate());
                    // 设置为当天的开始时间 00:00:00
                    queryWrapper.ge("start_time", start);
                    logger.info("设置开始时间过滤: {}", start);
                } catch (Exception e) {
                    logger.warn("开始日期格式错误: {}", request.getStartDate());
                }
            }
            
            if (StringUtils.hasText(request.getEndDate())) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date end = sdf.parse(request.getEndDate());
                    // 设置为当天的结束时间 23:59:59
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(end);
                    cal.set(Calendar.HOUR_OF_DAY, 23);
                    cal.set(Calendar.MINUTE, 59);
                    cal.set(Calendar.SECOND, 59);
                    cal.set(Calendar.MILLISECOND, 999);
                    queryWrapper.le("start_time", cal.getTime());
                    logger.info("设置结束时间过滤: {}", cal.getTime());
                } catch (Exception e) {
                    logger.warn("结束日期格式错误: {}", request.getEndDate());
                }
            }
            
            if (StringUtils.hasText(request.getJobCode())) {
                queryWrapper.eq("job_code", request.getJobCode());
                logger.info("设置作业代码过滤: {}", request.getJobCode());
            }
            
            // 添加排序
            queryWrapper.orderByAsc("start_time");
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            logger.info("查询到执行日志记录数: {}", logs.size());
            
            // 如果没有数据，返回空结果
            if (logs.isEmpty()) {
                logger.warn("查询范围内没有执行日志数据");
                Map<String, Object> emptyResult = new HashMap<>();
                emptyResult.put("dates", new ArrayList<>());
                emptyResult.put("successCounts", new ArrayList<>());
                emptyResult.put("failedCounts", new ArrayList<>());
                emptyResult.put("totalCounts", new ArrayList<>());
                emptyResult.put("avgDurations", new ArrayList<>());
                emptyResult.put("timeoutCounts", new ArrayList<>());
                return emptyResult;
            }
            
            // 按日期分组统计
            Map<String, Map<String, Integer>> dailyStats = new HashMap<>();
            Map<String, List<Long>> dailyDurations = new HashMap<>();
            Map<String, Integer> dailyTimeouts = new HashMap<>();
            
            for (JobExecutionLog log : logs) {
                String date = new SimpleDateFormat("yyyy-MM-dd").format(log.getStartTime());
                
                dailyStats.computeIfAbsent(date, k -> {
                    Map<String, Integer> stats = new HashMap<>();
                    stats.put("success", 0);
                    stats.put("failed", 0);
                    stats.put("total", 0);
                    return stats;
                });
                
                Map<String, Integer> stats = dailyStats.get(date);
                stats.put("total", stats.get("total") + 1);
                
                // 修复状态比较逻辑，使其与数据库中实际存储的状态值匹配
                String status = log.getStatus();
                if (status != null) {
                    // 统一转换为小写进行比较
                    String lowerStatus = status.toLowerCase();
                    if ("success".equals(lowerStatus) || "completed".equals(lowerStatus)) {
                        stats.put("success", stats.get("success") + 1);
                    } else if ("failed".equals(lowerStatus) || "error".equals(lowerStatus)) {
                        stats.put("failed", stats.get("failed") + 1);
                    } else if ("timeout".equals(lowerStatus)) {
                        dailyTimeouts.put(date, dailyTimeouts.getOrDefault(date, 0) + 1);
                    }
                }
                
                if (log.getDuration() != null) {
                    dailyDurations.computeIfAbsent(date, k -> new ArrayList<>()).add(log.getDuration().longValue());
                }
            }
            
            // 构建返回数据
            List<String> dates = new ArrayList<>(dailyStats.keySet());
            Collections.sort(dates);
            
            logger.info("统计到的日期数量: {}, 日期列表: {}", dates.size(), dates);
            
            List<Integer> successCounts = new ArrayList<>();
            List<Integer> failedCounts = new ArrayList<>();
            List<Integer> totalCounts = new ArrayList<>();
            List<Double> avgDurations = new ArrayList<>();
            List<Integer> timeoutCounts = new ArrayList<>();
            
            for (String date : dates) {
                Map<String, Integer> stats = dailyStats.get(date);
                successCounts.add(stats.get("success"));
                failedCounts.add(stats.get("failed"));
                totalCounts.add(stats.get("total"));
                
                List<Long> durations = dailyDurations.get(date);
                if (durations != null && !durations.isEmpty()) {
                    double avgDuration = durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
                    avgDurations.add(Math.round(avgDuration * 100.0) / 100.0);
                } else {
                    avgDurations.add(0.0);
                }
                
                timeoutCounts.add(dailyTimeouts.getOrDefault(date, 0));
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("dates", dates);
            result.put("successCounts", successCounts);
            result.put("failedCounts", failedCounts);
            result.put("totalCounts", totalCounts);
            result.put("avgDurations", avgDurations);
            result.put("timeoutCounts", timeoutCounts);
            
            logger.info("返回图表数据: 日期数量={}, 成功数量={}, 失败数量={}, 总数={}", 
                    dates.size(), successCounts.size(), failedCounts.size(), totalCounts.size());
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取执行趋势图表数据失败", e);
            throw new RuntimeException("获取执行趋势图表数据失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getJobGroupStats() {
        try {
            List<JobDef> allJobs = jobDefMapper.selectList(null);
            
            // 按分组统计
            Map<String, List<JobDef>> groupJobs = allJobs.stream()
                    .collect(Collectors.groupingBy(job -> job.getJobGroup() != null ? job.getJobGroup() : "DEFAULT"));
            
            List<Map<String, Object>> groups = new ArrayList<>();
            int totalJobs = 0;
            int totalActiveJobs = 0;
            int totalRunningJobs = 0;
            double totalSuccessRate = 0.0;
            long totalDuration = 0;
            
            for (Map.Entry<String, List<JobDef>> entry : groupJobs.entrySet()) {
                String groupName = entry.getKey();
                List<JobDef> jobs = entry.getValue();
                
                int groupTotalJobs = jobs.size();
                int groupActiveJobs = (int) jobs.stream().filter(job -> "active".equals(job.getStatus())).count();
                int groupRunningJobs = (int) jobs.stream().filter(job -> "running".equals(job.getStatus())).count();
                
                // 计算成功率（模拟数据）
                double successRate = groupTotalJobs > 0 ? 90.0 + Math.random() * 10.0 : 0.0;
                double avgDuration = 200.0 + Math.random() * 300.0;
                
                Map<String, Object> group = new HashMap<>();
                group.put("groupName", groupName);
                group.put("totalJobs", groupTotalJobs);
                group.put("activeJobs", groupActiveJobs);
                group.put("runningJobs", groupRunningJobs);
                group.put("successRate", Math.round(successRate * 100.0) / 100.0);
                group.put("avgDuration", Math.round(avgDuration));
                
                groups.add(group);
                
                totalJobs += groupTotalJobs;
                totalActiveJobs += groupActiveJobs;
                totalRunningJobs += groupRunningJobs;
                totalSuccessRate += successRate;
                totalDuration += avgDuration;
            }
            
            // 计算总体统计
            Map<String, Object> totalStats = new HashMap<>();
            totalStats.put("totalJobs", totalJobs);
            totalStats.put("activeJobs", totalActiveJobs);
            totalStats.put("runningJobs", totalRunningJobs);
            totalStats.put("avgSuccessRate", totalJobs > 0 ? Math.round((totalSuccessRate / groupJobs.size()) * 100.0) / 100.0 : 0.0);
            totalStats.put("avgDuration", totalJobs > 0 ? Math.round((double) totalDuration / groupJobs.size()) : 0);
            
            Map<String, Object> result = new HashMap<>();
            result.put("groups", groups);
            result.put("totalStats", totalStats);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取任务分组统计失败", e);
            throw new RuntimeException("获取任务分组统计失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getJobTypeDistribution() {
        try {
            List<JobDef> allJobs = jobDefMapper.selectList(null);
            
            // 按任务类型分组
            Map<String, List<JobDef>> typeJobs = allJobs.stream()
                    .collect(Collectors.groupingBy(job -> job.getJobType() != null ? job.getJobType() : "unknown"));
            
            List<Map<String, Object>> types = new ArrayList<>();
            int totalJobs = allJobs.size();
            
            for (Map.Entry<String, List<JobDef>> entry : typeJobs.entrySet()) {
                String jobType = entry.getKey();
                List<JobDef> jobs = entry.getValue();
                
                int count = jobs.size();
                double percentage = totalJobs > 0 ? (double) count / totalJobs * 100 : 0.0;
                double successRate = 85.0 + Math.random() * 15.0; // 模拟成功率
                
                Map<String, Object> type = new HashMap<>();
                type.put("jobType", jobType);
                type.put("count", count);
                type.put("percentage", Math.round(percentage * 100.0) / 100.0);
                type.put("successRate", Math.round(successRate * 100.0) / 100.0);
                
                types.add(type);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("types", types);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取任务类型分布失败", e);
            throw new RuntimeException("获取任务类型分布失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getExecutorPerformance(DashboardChartRequest request) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(request.getStartDate())) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date start = sdf.parse(request.getStartDate());
                    queryWrapper.ge("start_time", start);
                } catch (Exception e) {
                    logger.warn("开始日期格式错误: {}", request.getStartDate());
                }
            }
            
            if (StringUtils.hasText(request.getEndDate())) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date end = sdf.parse(request.getEndDate());
                    queryWrapper.le("start_time", end);
                } catch (Exception e) {
                    logger.warn("结束日期格式错误: {}", request.getEndDate());
                }
            }
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            // 按执行器分组统计
            Map<String, List<JobExecutionLog>> executorLogs = logs.stream()
                    .filter(log -> StringUtils.hasText(log.getExecutorAddress()))
                    .collect(Collectors.groupingBy(JobExecutionLog::getExecutorAddress));
            
            List<Map<String, Object>> executors = new ArrayList<>();
            
            for (Map.Entry<String, List<JobExecutionLog>> entry : executorLogs.entrySet()) {
                String executorAddress = entry.getKey();
                List<JobExecutionLog> executorLogList = entry.getValue();
                
                int executionCount = executorLogList.size();
                int successCount = (int) executorLogList.stream().filter(log -> "success".equals(log.getStatus())).count();
                int failedCount = (int) executorLogList.stream().filter(log -> "failed".equals(log.getStatus())).count();
                
                double avgDuration = executorLogList.stream()
                        .filter(log -> log.getDuration() != null)
                        .mapToLong(log -> log.getDuration().longValue())
                        .average()
                        .orElse(0.0);
                
                double successRate = executionCount > 0 ? (double) successCount / executionCount * 100 : 0.0;
                
                // 模拟资源使用情况
                double cpuUsage = 20.0 + Math.random() * 40.0;
                double memoryUsage = 30.0 + Math.random() * 50.0;
                int runningJobs = (int) (Math.random() * 10);
                
                Map<String, Object> executor = new HashMap<>();
                executor.put("executorAddress", executorAddress);
                executor.put("executionCount", executionCount);
                executor.put("successCount", successCount);
                executor.put("failedCount", failedCount);
                executor.put("avgDuration", Math.round(avgDuration));
                executor.put("successRate", Math.round(successRate * 100.0) / 100.0);
                executor.put("cpuUsage", Math.round(cpuUsage * 100.0) / 100.0);
                executor.put("memoryUsage", Math.round(memoryUsage * 100.0) / 100.0);
                executor.put("runningJobs", runningJobs);
                
                executors.add(executor);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("executors", executors);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取执行器性能统计失败", e);
            throw new RuntimeException("获取执行器性能统计失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getRecentExecutions(Integer limit) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            queryWrapper.orderByDesc("start_time");
            queryWrapper.last("LIMIT " + (limit != null ? limit : 10));
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            List<Map<String, Object>> recentExecutions = new ArrayList<>();
            for (JobExecutionLog log : logs) {
                Map<String, Object> execution = new HashMap<>();
                execution.put("logId", log.getLogId());
                execution.put("jobCode", log.getJobCode());
                //execution.put("jobName", log.getJobName());
                execution.put("batchNo", log.getBatchNo());
                execution.put("startTime", log.getStartTime());
                execution.put("endTime", log.getEndTime());
                execution.put("status", log.getStatus());
                execution.put("duration", log.getDuration());
                execution.put("errorMessage", log.getErrorMessage());
                execution.put("executorAddress", log.getExecutorAddress());
                
                recentExecutions.add(execution);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("recentExecutions", recentExecutions);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取最近执行记录失败", e);
            throw new RuntimeException("获取最近执行记录失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getRunningJobs() {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("status", "running");
            queryWrapper.orderByDesc("start_time");
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            List<Map<String, Object>> runningJobs = new ArrayList<>();
            for (JobExecutionLog log : logs) {
                Map<String, Object> job = new HashMap<>();
                job.put("logId", log.getLogId());
                job.put("jobCode", log.getJobCode());
                //job.put("jobName", log.getJobName());
                job.put("batchNo", log.getBatchNo());
                job.put("startTime", log.getStartTime());
                job.put("duration", log.getDuration());
                job.put("progress", 50 + (int)(Math.random() * 50)); // 模拟进度
                job.put("currentStep", "...");
                job.put("executorAddress", log.getExecutorAddress());
                
                // 估算结束时间
                if (log.getStartTime() != null && log.getDuration() != null) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(log.getStartTime());
                    cal.add(Calendar.SECOND, log.getDuration().intValue());
                    job.put("estimatedEndTime", cal.getTime());
                }
                
                runningJobs.add(job);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("runningJobs", runningJobs);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取当前运行任务失败", e);
            throw new RuntimeException("获取当前运行任务失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getAlerts(String level, Integer limit) {
        try {
            // 模拟告警数据
            List<Map<String, Object>> alerts = new ArrayList<>();
            
            Map<String, Object> alert1 = new HashMap<>();
            alert1.put("alertId", 1);
            alert1.put("level", "warning");
            alert1.put("title", "任务执行超时");
            alert1.put("message", "任务TEST002执行时间超过预期");
            alert1.put("jobCode", "TEST002");
            alert1.put("batchNo", "BATCH001_20250830_002");
            alert1.put("createTime", new Date());
            alert1.put("isRead", false);
            alerts.add(alert1);
            
            Map<String, Object> alert2 = new HashMap<>();
            alert2.put("alertId", 2);
            alert2.put("level", "error");
            alert2.put("message", "执行器192.168.1.101:8081连接异常");
            alert2.put("executorAddress", "192.168.1.101:8081");
            alert2.put("createTime", new Date());
            alert2.put("isRead", false);
            alerts.add(alert2);
            
            // 根据级别过滤
            if (StringUtils.hasText(level)) {
                alerts = alerts.stream()
                        .filter(alert -> level.equals(alert.get("level")))
                        .collect(Collectors.toList());
            }
            
            // 限制数量
            if (limit != null && limit > 0) {
                alerts = alerts.stream().limit(limit).collect(Collectors.toList());
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("alerts", alerts);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取系统告警信息失败", e);
            throw new RuntimeException("获取系统告警信息失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getDailyReport(String date) {
        try {
            Date reportDate;
            if (StringUtils.hasText(date)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    reportDate = sdf.parse(date);
                } catch (Exception e) {
                    logger.warn("日期格式错误: {}", date);
                    reportDate = new Date();
                }
            } else {
                reportDate = new Date();
            }
            
            // 获取指定日期的执行记录
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            queryWrapper.ge("start_time", getStartOfDay(reportDate));
            queryWrapper.lt("start_time", getEndOfDay(reportDate));
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            int totalExecutions = logs.size();
            int successExecutions = (int) logs.stream().filter(log -> "success".equals(log.getStatus())).count();
            int failedExecutions = (int) logs.stream().filter(log -> "failed".equals(log.getStatus())).count();
            double successRate = totalExecutions > 0 ? (double) successExecutions / totalExecutions * 100 : 0.0;
            
            double avgDuration = logs.stream()
                    .filter(log -> log.getDuration() != null)
                    .mapToLong(log -> log.getDuration().longValue())
                    .average()
                    .orElse(0.0);
            
            long totalDuration = logs.stream()
                    .filter(log -> log.getDuration() != null)
                    .mapToLong(log -> log.getDuration().longValue())
                    .sum();
            
            // 构建摘要
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalExecutions", totalExecutions);
            summary.put("successExecutions", successExecutions);
            summary.put("failedExecutions", failedExecutions);
            summary.put("successRate", Math.round(successRate * 100.0) / 100.0);
            summary.put("avgDuration", Math.round(avgDuration));
            summary.put("totalDuration", formatDuration(totalDuration));
            
            // 获取热门任务
            Map<String, List<JobExecutionLog>> jobLogs = logs.stream()
                    .collect(Collectors.groupingBy(JobExecutionLog::getJobCode));
            
            List<Map<String, Object>> topJobs = new ArrayList<>();
            for (Map.Entry<String, List<JobExecutionLog>> entry : jobLogs.entrySet()) {
                String jobCode = entry.getKey();
                List<JobExecutionLog> jobLogList = entry.getValue();
                
                int executionCount = jobLogList.size();
                int successCount = (int) jobLogList.stream().filter(log -> "success".equals(log.getStatus())).count();
                double jobAvgDuration = jobLogList.stream()
                        .filter(log -> log.getDuration() != null)
                        .mapToLong(log -> log.getDuration().longValue())
                        .average()
                        .orElse(0.0);
                
                Map<String, Object> job = new HashMap<>();
                job.put("jobCode", jobCode);
                //job.put("jobName", jobLogList.get(0).getJobName());
                job.put("executionCount", executionCount);
                job.put("successCount", successCount);
                job.put("avgDuration", Math.round(jobAvgDuration));
                
                topJobs.add(job);
            }
            
            // 按执行次数排序
            topJobs.sort((a, b) -> Integer.compare((Integer) b.get("executionCount"), (Integer) a.get("executionCount")));
            if (topJobs.size() > 10) {
                topJobs = topJobs.subList(0, 10);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("date", new SimpleDateFormat("yyyy-MM-dd").format(reportDate));
            result.put("summary", summary);
            result.put("topJobs", topJobs);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取日报表失败", e);
            throw new RuntimeException("获取日报表失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getMonthlyReport(Integer year, Integer month) {
        try {
            Calendar cal = Calendar.getInstance();
            if (year != null) cal.set(Calendar.YEAR, year);
            if (month != null) cal.set(Calendar.MONTH, month - 1);
            
            cal.set(Calendar.DAY_OF_MONTH, 1);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            Date startDate = cal.getTime();
            
            cal.add(Calendar.MONTH, 1);
            Date endDate = cal.getTime();
            
            // 获取指定月份的执行记录
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            queryWrapper.ge("start_time", startDate);
            queryWrapper.lt("start_time", endDate);
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            int totalExecutions = logs.size();
            int successExecutions = (int) logs.stream().filter(log -> "success".equals(log.getStatus())).count();
            int failedExecutions = (int) logs.stream().filter(log -> "failed".equals(log.getStatus())).count();
            double successRate = totalExecutions > 0 ? (double) successExecutions / totalExecutions * 100 : 0.0;
            
            double avgDuration = logs.stream()
                    .filter(log -> log.getDuration() != null)
                    .mapToLong(log -> log.getDuration().longValue())
                    .average()
                    .orElse(0.0);
            
            long totalDuration = logs.stream()
                    .filter(log -> log.getDuration() != null)
                    .mapToLong(log -> log.getDuration().longValue())
                    .sum();
            
            // 构建摘要
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalExecutions", totalExecutions);
            summary.put("successExecutions", successExecutions);
            summary.put("failedExecutions", failedExecutions);
            summary.put("successRate", Math.round(successRate * 100.0) / 100.0);
            summary.put("avgDuration", Math.round(avgDuration));
            summary.put("totalDuration", formatDuration(totalDuration));
            
            Map<String, Object> result = new HashMap<>();
            result.put("year", cal.get(Calendar.YEAR));
            result.put("month", cal.get(Calendar.MONTH) + 1);
            result.put("summary", summary);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取月报表失败", e);
            throw new RuntimeException("获取月报表失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取当天开始时间
     */
    private Date getStartOfDay() {
        return getStartOfDay(new Date());
    }
    
    /**
     * 获取指定日期的开始时间
     */
    private Date getStartOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
    
    /**
     * 获取指定日期的结束时间
     */
    private Date getEndOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
    }
    
    /**
     * 格式化持续时间
     */
    private String formatDuration(long seconds) {
        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        return hours + "小时" + minutes + "分钟";
    }
} 