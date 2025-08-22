package com.aia.gdp.service;

import com.aia.gdp.dto.JobLogListRequest;
import com.aia.gdp.dto.BatchLogDeleteRequest;
import com.aia.gdp.dto.LogCleanupRequest;
import com.aia.gdp.dto.LogExportRequest;
import com.aia.gdp.model.JobExecutionLog;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface JobExecutionLogService {
    void save(JobExecutionLog log);
    List<JobExecutionLog> listByJobCode(String jobCode);
    JobExecutionLog getById(Long logId);
    
    // 新增方法
    Map<String, Object> getJobLogList(JobLogListRequest request);
    JobExecutionLog getJobLogDetail(Long logId);
    boolean deleteJobLog(Long logId);
    Map<String, Object> batchDeleteJobLogs(BatchLogDeleteRequest request);
    Map<String, Object> cleanupJobLogs(LogCleanupRequest request);
    Map<String, Object> getJobLogStats(String startDate, String endDate, String jobCode);
    Map<String, Object> exportJobLogs(LogExportRequest request);
    Map<String, Object> getRealtimeJobLogs(String jobCode, String batchNo);
    Map<String, Object> getExecutionTrend(String startDate, String endDate, String groupBy, String jobCode);
    Map<String, Object> getJobPerformance(String startDate, String endDate, String jobCode);
    
    // 新增：状态更新相关方法
    /**
     * 更新执行状态
     */
    boolean updateStatus(Long logId, String status, String reason);

    /**
     * 更新执行时间
     */
    boolean updateExecutionTime(Long logId, Date endTime, Integer duration);
    
    /**
     * 获取最新的运行中日志
     */
    JobExecutionLog getLatestRunningLog(String jobCode);
    
    /**
     * 获取任务的所有执行日志
     */
    List<JobExecutionLog> getExecutionHistory(String jobCode, int limit);
} 