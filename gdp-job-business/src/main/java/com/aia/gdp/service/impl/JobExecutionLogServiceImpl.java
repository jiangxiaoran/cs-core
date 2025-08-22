package com.aia.gdp.service.impl;

import com.aia.gdp.dto.JobLogListRequest;
import com.aia.gdp.dto.BatchLogDeleteRequest;
import com.aia.gdp.dto.LogCleanupRequest;
import com.aia.gdp.dto.LogExportRequest;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.mapper.JobExecutionLogMapper;
import com.aia.gdp.service.JobExecutionLogService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;
import java.text.SimpleDateFormat;

@Service
public class JobExecutionLogServiceImpl implements JobExecutionLogService {
    
    private static final Logger logger = LoggerFactory.getLogger(JobExecutionLogServiceImpl.class);
    
    @Autowired
    private JobExecutionLogMapper jobExecutionLogMapper;

    @Override
    public void save(JobExecutionLog log) {
        // 确保设置创建时间
        if (log.getCreateTime() == null) {
            log.setCreateTime(new java.util.Date());
        }
        // 设置默认值
        if (log.getNotifyStatus() == null) {
            log.setNotifyStatus("PENDING");
        }
        if (log.getRetryCount() == null) {
            log.setRetryCount(0);
        }
        // 使用自定义的insert方法，避免MyBatis-Plus的自动映射问题
        jobExecutionLogMapper.insert(log);
    }

    @Override
    public List<JobExecutionLog> listByJobCode(String jobCode) {
        // 使用自定义的selectByJobCode方法
        // 注意：这里返回的仍然是JobExecutionLog对象，包含Java Date
        // 如果需要在前端显示格式化时间，建议在前端进行格式化
        // 或者创建一个DTO类来返回格式化的数据
        return jobExecutionLogMapper.selectByJobCode(jobCode);
    }

    @Override
    public JobExecutionLog getById(Long logId) {
        // 使用MyBatis-Plus的selectById方法
        return jobExecutionLogMapper.selectById(logId);
    }
    
    @Override
    public Map<String, Object> getJobLogList(JobLogListRequest request) {
        try {
            // 构建查询条件
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(request.getJobCode())) {
                queryWrapper.like("job_code", request.getJobCode());
            }
            
            if (StringUtils.hasText(request.getBatchNo())) {
                queryWrapper.like("batch_no", request.getBatchNo());
            }
            
            if (StringUtils.hasText(request.getStatus())) {
                queryWrapper.eq("status", request.getStatus());
            }
            
            if (request.getStartTime() != null) {
                queryWrapper.ge("start_time", request.getStartTime());
            }
            
            if (request.getEndTime() != null) {
                queryWrapper.le("start_time", request.getEndTime());
            }
            
            if (StringUtils.hasText(request.getExecutorAddress())) {
                queryWrapper.like("executor_address", request.getExecutorAddress());
            }
            
            // 动态排序，默认按logId降序排列
            String orderBy = request.getOrderBy() != null ? request.getOrderBy() : "logId";
            String orderDirection = request.getOrderDirection() != null ? request.getOrderDirection() : "desc";
            
            // 将Java字段名映射到数据库列名
            String dbColumnName = mapFieldToDbColumn(orderBy);
            
            if ("desc".equalsIgnoreCase(orderDirection)) {
                queryWrapper.orderByDesc(dbColumnName);
            } else {
                queryWrapper.orderByAsc(dbColumnName);
            }
            
            // 分页查询
            Page<JobExecutionLog> page = new Page<>(request.getCurrent(), request.getPageSize());
            IPage<JobExecutionLog> result = jobExecutionLogMapper.selectPage(page, queryWrapper);
            
            // 将Java Date对象转换为标准时间字符串格式
            List<Map<String, Object>> formattedList = new ArrayList<>();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            for (JobExecutionLog log : result.getRecords()) {
                Map<String, Object> formattedLog = new HashMap<>();
                formattedLog.put("logId", log.getLogId());
                formattedLog.put("jobCode", log.getJobCode());
                formattedLog.put("batchNo", log.getBatchNo());
                formattedLog.put("executorProc", log.getExecutorProc());
                formattedLog.put("executorAddress", log.getExecutorAddress());
                
                // 格式化开始时间
                if (log.getStartTime() != null) {
                    formattedLog.put("startTime", dateFormat.format(log.getStartTime()));
                } else {
                    formattedLog.put("startTime", null);
                }
                
                // 格式化结束时间
                if (log.getEndTime() != null) {
                    formattedLog.put("endTime", dateFormat.format(log.getEndTime()));
                } else {
                    formattedLog.put("endTime", null);
                }
                
                formattedLog.put("status", log.getStatus());
                formattedLog.put("errorMessage", log.getErrorMessage());
                formattedLog.put("duration", log.getDuration());
                formattedLog.put("notifyStatus", log.getNotifyStatus());
                formattedLog.put("retryCount", log.getRetryCount());
                formattedLog.put("createTime", log.getCreateTime() != null ? dateFormat.format(log.getCreateTime()) : null);
                
                formattedList.add(formattedLog);
            }
            
            // 构建返回结果
            Map<String, Object> data = new HashMap<>();
            data.put("list", formattedList);
            data.put("total", result.getTotal());
            data.put("current", result.getCurrent());
            data.put("pageSize", result.getSize());
            data.put("pages", result.getPages());
            
            return data;
            
        } catch (Exception e) {
            logger.error("获取执行日志列表失败", e);
            throw new RuntimeException("获取执行日志列表失败: " + e.getMessage());
        }
    }
    
    @Override
    public JobExecutionLog getJobLogDetail(Long logId) {
        try {
            JobExecutionLog log = jobExecutionLogMapper.selectById(logId);
            if (log == null) {
                throw new RuntimeException("执行日志不存在");
            }
            
            // 注意：这里返回的仍然是JobExecutionLog对象，包含Java Date
            // 如果需要在前端显示格式化时间，建议在前端进行格式化
            // 或者创建一个DTO类来返回格式化的数据
            return log;
        } catch (Exception e) {
            logger.error("获取执行日志详情失败, logId: {}", logId, e);
            throw new RuntimeException("获取执行日志详情失败: " + e.getMessage());
        }
    }
    
    @Override
    public boolean deleteJobLog(Long logId) {
        try {
            JobExecutionLog log = jobExecutionLogMapper.selectById(logId);
            if (log == null) {
                throw new RuntimeException("执行日志不存在");
            }
            
            int result = jobExecutionLogMapper.deleteById(logId);
            return result > 0;
            
        } catch (Exception e) {
            logger.error("删除执行日志失败, logId: {}", logId, e);
            throw new RuntimeException("删除执行日志失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> batchDeleteJobLogs(BatchLogDeleteRequest request) {
        try {
            List<Long> logIds = request.getLogIds();
            if (logIds == null || logIds.isEmpty()) {
                throw new RuntimeException("日志ID列表不能为空");
            }
            
            int successCount = 0;
            int failedCount = 0;
            
            for (Long logId : logIds) {
                try {
                    boolean result = deleteJobLog(logId);
                    if (result) {
                        successCount++;
                    } else {
                        failedCount++;
                    }
                } catch (Exception e) {
                    logger.error("批量删除执行日志失败, logId: {}", logId, e);
                    failedCount++;
                }
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("successCount", successCount);
            result.put("failedCount", failedCount);
            
            return result;
            
        } catch (Exception e) {
            logger.error("批量删除执行日志失败", e);
            throw new RuntimeException("批量删除执行日志失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> cleanupJobLogs(LogCleanupRequest request) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (request.getBeforeDate() != null) {
                queryWrapper.lt("start_time", request.getBeforeDate());
            }
            
            if (request.getStatus() != null && !request.getStatus().isEmpty()) {
                queryWrapper.in("status", request.getStatus());
            }
            
            if (request.getJobCodes() != null && !request.getJobCodes().isEmpty()) {
                queryWrapper.in("job_code", request.getJobCodes());
            }
            
            // 查询要删除的记录数
            List<JobExecutionLog> logsToDelete = jobExecutionLogMapper.selectList(queryWrapper);
            int deletedCount = logsToDelete.size();
            
            // 执行删除
            int result = jobExecutionLogMapper.delete(queryWrapper);
            
            Map<String, Object> data = new HashMap<>();
            data.put("deletedCount", deletedCount);
            data.put("freedSpace", (deletedCount * 1024) + "KB"); // 估算释放空间
            
            return data;
            
        } catch (Exception e) {
            logger.error("清理执行日志失败", e);
            throw new RuntimeException("清理执行日志失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getJobLogStats(String startDate, String endDate, String jobCode) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(startDate)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date start = sdf.parse(startDate);
                    queryWrapper.ge("start_time", start);
                } catch (Exception e) {
                    logger.warn("开始日期格式错误: {}", startDate);
                }
            }
            
            if (StringUtils.hasText(endDate)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date end = sdf.parse(endDate);
                    queryWrapper.le("start_time", end);
                } catch (Exception e) {
                    logger.warn("结束日期格式错误: {}", endDate);
                }
            }
            
            if (StringUtils.hasText(jobCode)) {
                queryWrapper.eq("job_code", jobCode);
            }
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            // 计算统计信息
            int totalCount = logs.size();
            int successCount = 0;
            int failedCount = 0;
            int runningCount = 0;
            int timeoutCount = 0;
            long totalDuration = 0;
            long maxDuration = 0;
            long minDuration = Long.MAX_VALUE;
            
            for (JobExecutionLog log : logs) {
                if ("success".equals(log.getStatus())) {
                    successCount++;
                } else if ("failed".equals(log.getStatus())) {
                    failedCount++;
                } else if ("running".equals(log.getStatus())) {
                    runningCount++;
                } else if ("timeout".equals(log.getStatus())) {
                    timeoutCount++;
                }
                
                if (log.getDuration() != null) {
                    totalDuration += log.getDuration();
                    maxDuration = Math.max(maxDuration, log.getDuration());
                    minDuration = Math.min(minDuration, log.getDuration());
                }
            }
            
            double avgDuration = totalCount > 0 ? (double) totalDuration / totalCount : 0;
            double successRate = totalCount > 0 ? (double) successCount / totalCount * 100 : 0;
            
            Map<String, Object> stats = new HashMap<>();
            stats.put("totalCount", totalCount);
            stats.put("successCount", successCount);
            stats.put("failedCount", failedCount);
            stats.put("runningCount", runningCount);
            stats.put("timeoutCount", timeoutCount);
            stats.put("avgDuration", Math.round(avgDuration));
            stats.put("maxDuration", maxDuration);
            stats.put("minDuration", minDuration == Long.MAX_VALUE ? 0 : minDuration);
            stats.put("successRate", Math.round(successRate * 100.0) / 100.0);
            
            return stats;
            
        } catch (Exception e) {
            logger.error("获取执行日志统计信息失败", e);
            throw new RuntimeException("获取执行日志统计信息失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> exportJobLogs(LogExportRequest request) {
        try {
            // 这里应该实现实际的导出逻辑
            // 暂时返回模拟数据
            Map<String, Object> result = new HashMap<>();
            result.put("downloadUrl", "http://localhost:8081/api/v1/files/exports/job-logs-" + 
                      new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".xlsx");
            result.put("fileName", "job-logs-" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".xlsx");
            result.put("fileSize", "2.5MB");
            result.put("expireTime", new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000));
            
            return result;
            
        } catch (Exception e) {
            logger.error("导出执行日志失败", e);
            throw new RuntimeException("导出执行日志失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getRealtimeJobLogs(String jobCode, String batchNo) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(jobCode)) {
                queryWrapper.eq("job_code", jobCode);
            }
            
            if (StringUtils.hasText(batchNo)) {
                queryWrapper.eq("batch_no", batchNo);
            }
            
            // 获取运行中的任务
            queryWrapper.eq("status", "running");
            List<JobExecutionLog> runningJobs = jobExecutionLogMapper.selectList(queryWrapper);
            
            // 获取最近完成的任务
            queryWrapper.clear();
            queryWrapper.ne("status", "running");
            queryWrapper.orderByDesc("start_time");
            queryWrapper.last("LIMIT 10");
            List<JobExecutionLog> recentCompleted = jobExecutionLogMapper.selectList(queryWrapper);
            
            Map<String, Object> result = new HashMap<>();
            result.put("runningJobs", runningJobs);
            result.put("recentCompleted", recentCompleted);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取实时执行日志失败", e);
            throw new RuntimeException("获取实时执行日志失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getExecutionTrend(String startDate, String endDate, String groupBy, String jobCode) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(startDate)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date start = sdf.parse(startDate);
                    queryWrapper.ge("start_time", start);
                } catch (Exception e) {
                    logger.warn("开始日期格式错误: {}", startDate);
                }
            }
            
            if (StringUtils.hasText(endDate)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date end = sdf.parse(endDate);
                    queryWrapper.le("start_time", end);
                } catch (Exception e) {
                    logger.warn("结束日期格式错误: {}", endDate);
                }
            }
            
            if (StringUtils.hasText(jobCode)) {
                queryWrapper.eq("job_code", jobCode);
            }
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            // 按日期分组统计
            Map<String, Map<String, Integer>> dailyStats = new HashMap<>();
            Map<String, List<Long>> dailyDurations = new HashMap<>();
            
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
                
                if ("success".equals(log.getStatus())) {
                    stats.put("success", stats.get("success") + 1);
                } else if ("failed".equals(log.getStatus())) {
                    stats.put("failed", stats.get("failed") + 1);
                }
                
                if (log.getDuration() != null) {
                    dailyDurations.computeIfAbsent(date, k -> new ArrayList<>()).add(log.getDuration().longValue());
                }
            }
            
            // 构建返回数据
            List<String> dates = new ArrayList<>(dailyStats.keySet());
            Collections.sort(dates);
            
            List<Integer> successCounts = new ArrayList<>();
            List<Integer> failedCounts = new ArrayList<>();
            List<Integer> totalCounts = new ArrayList<>();
            List<Double> avgDurations = new ArrayList<>();
            
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
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("dates", dates);
            result.put("successCounts", successCounts);
            result.put("failedCounts", failedCounts);
            result.put("totalCounts", totalCounts);
            result.put("avgDurations", avgDurations);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取执行趋势分析失败", e);
            throw new RuntimeException("获取执行趋势分析失败: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> getJobPerformance(String startDate, String endDate, String jobCode) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(startDate)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date start = sdf.parse(startDate);
                    queryWrapper.ge("start_time", start);
                } catch (Exception e) {
                    logger.warn("开始日期格式错误: {}", startDate);
                }
            }
            
            if (StringUtils.hasText(endDate)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    Date end = sdf.parse(endDate);
                    queryWrapper.le("start_time", end);
                } catch (Exception e) {
                    logger.warn("结束日期格式错误: {}", endDate);
                }
            }
            
            if (StringUtils.hasText(jobCode)) {
                queryWrapper.eq("job_code", jobCode);
            }
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            // 按任务代码分组统计
            Map<String, Map<String, Object>> jobStats = new HashMap<>();
            Map<String, Map<String, Object>> executorStats = new HashMap<>();
            
            for (JobExecutionLog log : logs) {
                // 任务统计
                jobStats.computeIfAbsent(log.getJobCode(), k -> {
                    Map<String, Object> stats = new HashMap<>();
                    stats.put("executionCount", 0);
                    stats.put("successCount", 0);
                    stats.put("failedCount", 0);
                    stats.put("durations", new ArrayList<Long>());
                    stats.put("retryCounts", new ArrayList<Integer>());
                    return stats;
                });
                
                Map<String, Object> jobStat = jobStats.get(log.getJobCode());
                jobStat.put("executionCount", (Integer) jobStat.get("executionCount") + 1);
                
                if ("success".equals(log.getStatus())) {
                    jobStat.put("successCount", (Integer) jobStat.get("successCount") + 1);
                } else if ("failed".equals(log.getStatus())) {
                    jobStat.put("failedCount", (Integer) jobStat.get("failedCount") + 1);
                }
                
                if (log.getDuration() != null) {
                    ((List<Long>) jobStat.get("durations")).add(log.getDuration().longValue());
                }
                
                if (log.getRetryCount() != null) {
                    ((List<Integer>) jobStat.get("retryCounts")).add(log.getRetryCount());
                }
                
                // 执行器统计
                if (StringUtils.hasText(log.getExecutorAddress())) {
                    executorStats.computeIfAbsent(log.getExecutorAddress(), k -> {
                        Map<String, Object> stats = new HashMap<>();
                        stats.put("executionCount", 0);
                        stats.put("successCount", 0);
                        stats.put("failedCount", 0);
                        stats.put("durations", new ArrayList<Long>());
                        return stats;
                    });
                    
                    Map<String, Object> executorStat = executorStats.get(log.getExecutorAddress());
                    executorStat.put("executionCount", (Integer) executorStat.get("executionCount") + 1);
                    
                    if ("success".equals(log.getStatus())) {
                        executorStat.put("successCount", (Integer) executorStat.get("successCount") + 1);
                    } else if ("failed".equals(log.getStatus())) {
                        executorStat.put("failedCount", (Integer) executorStat.get("failedCount") + 1);
                    }
                    
                    if (log.getDuration() != null) {
                        ((List<Long>) executorStat.get("durations")).add(log.getDuration().longValue());
                    }
                }
            }
            
            // 构建任务性能数据
            List<Map<String, Object>> jobPerformance = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : jobStats.entrySet()) {
                Map<String, Object> jobStat = entry.getValue();
                List<Long> durations = (List<Long>) jobStat.get("durations");
                List<Integer> retryCounts = (List<Integer>) jobStat.get("retryCounts");
                
                double avgDuration = durations.isEmpty() ? 0 : durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
                double avgRetryCount = retryCounts.isEmpty() ? 0 : retryCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
                double successRate = (Integer) jobStat.get("executionCount") > 0 ? 
                    (double) (Integer) jobStat.get("successCount") / (Integer) jobStat.get("executionCount") * 100 : 0;
                
                Map<String, Object> performance = new HashMap<>();
                performance.put("jobCode", entry.getKey());
                performance.put("executionCount", jobStat.get("executionCount"));
                performance.put("successCount", jobStat.get("successCount"));
                performance.put("failedCount", jobStat.get("failedCount"));
                performance.put("avgDuration", Math.round(avgDuration));
                performance.put("maxDuration", durations.isEmpty() ? 0 : Collections.max(durations));
                performance.put("minDuration", durations.isEmpty() ? 0 : Collections.min(durations));
                performance.put("successRate", Math.round(successRate * 100.0) / 100.0);
                performance.put("avgRetryCount", Math.round(avgRetryCount * 100.0) / 100.0);
                
                jobPerformance.add(performance);
            }
            
            // 构建执行器性能数据
            List<Map<String, Object>> executorPerformance = new ArrayList<>();
            for (Map.Entry<String, Map<String, Object>> entry : executorStats.entrySet()) {
                Map<String, Object> executorStat = entry.getValue();
                List<Long> durations = (List<Long>) executorStat.get("durations");
                
                double avgDuration = durations.isEmpty() ? 0 : durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
                double successRate = (Integer) executorStat.get("executionCount") > 0 ? 
                    (double) (Integer) executorStat.get("successCount") / (Integer) executorStat.get("executionCount") * 100 : 0;
                
                Map<String, Object> performance = new HashMap<>();
                performance.put("executorAddress", entry.getKey());
                performance.put("executionCount", executorStat.get("executionCount"));
                performance.put("successCount", executorStat.get("successCount"));
                performance.put("failedCount", executorStat.get("failedCount"));
                performance.put("avgDuration", Math.round(avgDuration));
                performance.put("successRate", Math.round(successRate * 100.0) / 100.0);
                
                executorPerformance.add(performance);
            }
            
            Map<String, Object> result = new HashMap<>();
            result.put("jobPerformance", jobPerformance);
            result.put("executorPerformance", executorPerformance);
            
            return result;
            
        } catch (Exception e) {
            logger.error("获取任务性能分析失败", e);
            throw new RuntimeException("获取任务性能分析失败: " + e.getMessage());
        }
    }
    
    // 新增：状态更新相关方法实现
    
    @Override
    public boolean updateStatus(Long logId, String status, String reason) {
        try {
            // 🎯 先查询现有记录，然后更新，避免参数绑定问题
            JobExecutionLog existingLog = jobExecutionLogMapper.selectById(logId);
            if (existingLog == null) {
                logger.warn("执行日志记录不存在: logId={}", logId);
                return false;
            }
            
            // 更新字段
            existingLog.setStatus(status);
            existingLog.setErrorMessage(reason);
            
            // 如果状态是最终状态，设置结束时间
            if ("success".equals(status) || "failed".equals(status) || 
                "stopped".equals(status) || "cancelled".equals(status)) {
                existingLog.setEndTime(new Date());
            }
            
            int result = jobExecutionLogMapper.updateById(existingLog);
            if (result > 0) {
                logger.info("更新执行日志状态成功: logId={}, status={}, reason={}", logId, status, reason);
                return true;
            } else {
                logger.warn("更新执行日志状态失败: logId={}, status={}, reason={}", logId, status, reason);
                return false;
            }
        } catch (Exception e) {
            logger.error("更新执行日志状态异常: logId={}, status={}, reason={}", logId, status, reason, e);
            return false;
        }
    }

    @Override
    public boolean updateExecutionTime(Long logId, Date endTime, Integer duration) {
        try {
            // 🎯 先查询现有记录，然后更新，避免参数绑定问题
            JobExecutionLog existingLog = jobExecutionLogMapper.selectById(logId);
            if (existingLog == null) {
                logger.warn("执行日志记录不存在: logId={}", logId);
                return false;
            }
            
            // 更新字段
            existingLog.setEndTime(endTime);
            existingLog.setDuration(duration);
            
            // 根据结束时间和执行时长计算开始时间
            if (endTime != null && duration != null && duration > 0) {
                // duration是以秒为单位，需要转换为毫秒
                long durationMs = duration * 1000L;
                long startTimeMs = endTime.getTime() - durationMs;
                Date startTime = new Date(startTimeMs);
                existingLog.setStartTime(startTime);
                
                logger.debug("计算开始时间: endTime={}, duration={}秒, startTime={}", 
                           endTime, duration, startTime);
            }
            
            int result = jobExecutionLogMapper.updateById(existingLog);
            if (result > 0) {
                logger.info("更新执行时间成功: logId={}, endTime={}, duration={}秒", logId, endTime, duration);
                return true;
            } else {
                logger.warn("更新执行时间失败: logId={}, endTime={}, duration={}秒", logId, endTime, duration);
                return false;
            }
        } catch (Exception e) {
            logger.error("更新执行时间异常: logId={}, endTime={}, duration={}秒", logId, endTime, duration, e);
            return false;
        }
    }
    
    @Override
    public JobExecutionLog getLatestRunningLog(String jobCode) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("job_code", jobCode)
                       .eq("status", "running")
                       .orderByDesc("start_time")
                       .last("LIMIT 1");
            
            return jobExecutionLogMapper.selectOne(queryWrapper);
        } catch (Exception e) {
            logger.error("获取最新运行中日志失败: {}", jobCode, e);
            return null;
        }
    }
    
    @Override
    public List<JobExecutionLog> getExecutionHistory(String jobCode, int limit) {
        try {
            QueryWrapper<JobExecutionLog> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("job_code", jobCode)
                       .orderByDesc("start_time")
                       .last("LIMIT " + limit);
            
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectList(queryWrapper);
            
            // 注意：这里返回的仍然是JobExecutionLog对象，包含Java Date
            // 如果需要在前端显示格式化时间，建议在前端进行格式化
            // 或者创建一个DTO类来返回格式化的数据
            return logs;
        } catch (Exception e) {
            logger.error("获取执行历史失败: {}", jobCode, e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 将Java字段名映射到数据库列名
     */
    private String mapFieldToDbColumn(String fieldName) {
        if (fieldName == null) {
            return "log_id"; // 默认列名
        }
        
        switch (fieldName) {
            case "logId":
                return "log_id";
            case "jobCode":
                return "job_code";
            case "batchNo":
                return "batch_no";
            case "executorProc":
                return "executor_proc";
            case "executorAddress":
                return "executor_address";
            case "startTime":
                return "start_time";
            case "endTime":
                return "end_time";
            case "errorMessage":
                return "error_message";
            case "notifyStatus":
                return "notify_status";
            case "retryCount":
                return "retry_count";
            case "createTime":
                return "create_time";
            default:
                // 如果字段名不匹配，尝试转换为下划线格式
                return fieldName.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
        }
    }
} 