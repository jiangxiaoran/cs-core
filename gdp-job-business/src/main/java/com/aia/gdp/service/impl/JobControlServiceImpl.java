package com.aia.gdp.service.impl;

import com.aia.gdp.dto.BatchJobRequest;
import com.aia.gdp.dto.JobStartRequest;
import com.aia.gdp.model.*;
import com.aia.gdp.service.JobControlService;
import com.aia.gdp.service.JobDefService;
import com.aia.gdp.service.JobExecutionLogService;
import com.aia.gdp.event.JobControlEvent;
import com.aia.gdp.dto.JobExecutionResult;
import com.aia.gdp.mapper.JobDefMapper;
import com.aia.gdp.mapper.JobExecutionLogMapper;
import com.aia.gdp.common.Utils;
import com.aia.gdp.common.CacheKeyUtils;
import com.aia.gdp.handler.WorkerHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 作业控制服务实现类
 * 提供作业队列的暂停、恢复、终止取消等控制功能
 * 采用事件驱动架构，避免循环依赖
 * 
 * 重构说明：
 * - 使用CacheKeyUtils统一管理缓存键值
 * - 支持批次号，避免多次触发时的状态混乱
 * - 键值格式：jobCode_batchNo, groupName_batchNo
 *
 * @author andy
 * @date 2025-08-18
 * @version 2.0
 */
@Service
public class JobControlServiceImpl implements JobControlService {
    
    private static final Logger logger = LoggerFactory.getLogger(JobControlServiceImpl.class);
    
    @Autowired
    private JobDefService jobDefService;
    
    @Autowired
    private JobExecutionLogService jobExecutionLogService;
    
    @Autowired
    private JobExecutionLogMapper jobExecutionLogMapper;
    
    @Autowired
    private WorkerHandler workerHandler;
    
    @Autowired
    private ApplicationEventPublisher eventPublisher;
    
    // ==================== 统一数据管理 ====================
    
    // 作业组数据缓存（整合状态信息）
    // 键值：groupName + "_" + batchNo
    private final Map<String, JobGroupData> jobGroupDataCache = new ConcurrentHashMap<>();
    
    // 作业定义数据缓存（整合状态信息）
    // 键值：jobCode + "_" + batchNo
    private final Map<String, JobData> jobDefCache = new ConcurrentHashMap<>();
    
    // 活跃作业组集合
    // 键值：groupName + "_" + batchNo
    private final Set<String> activeJobGroups = ConcurrentHashMap.newKeySet();
    
    // 数据初始化标志
    private final AtomicBoolean dataInitialized = new AtomicBoolean(false);
    
    // 数据初始化锁
    private final ReentrantReadWriteLock dataInitLock = new ReentrantReadWriteLock();
    
    // 简单的状态验证
    private static final Set<ExecutionStatus> VALID_STATUSES = 
        Set.of(ExecutionStatus.PENDING,ExecutionStatus.RUNNING, ExecutionStatus.PAUSED, ExecutionStatus.STOPPED,
               ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED);
    
    // 作业组执行锁
    // 键值：groupName + "_" + batchNo
    private final Map<String, ReentrantReadWriteLock> groupLocks = new ConcurrentHashMap<>();
    
    // 作业执行锁
    // 键值：jobCode + "_" + batchNo
    private final Map<String, ReentrantReadWriteLock> jobLocks = new ConcurrentHashMap<>();
    
    // 执行统计
    // 键值：groupName + "_" + batchNo
    private final Map<String, GroupExecutionStatistics> groupStatistics = new ConcurrentHashMap<>();
    private final SystemExecutionStatistics systemStatistics = new SystemExecutionStatistics();
    
    // 线程池
    private final ExecutorService controlExecutor = Executors.newFixedThreadPool(10);
    
    // 系统启动时间
    private final long systemStartTime = System.currentTimeMillis();
    
    public JobControlServiceImpl() {
        systemStatistics.setSystemStartTime(systemStartTime);
        
        // 系统启动时只初始化基础状态，不加载所有作业数据
        // 作业数据将在实际需要时按需加载
        logger.info("JobControlServiceImpl 初始化完成，采用按需加载策略");
    }
    
    /**
     * 获取所有活跃的作业组
     */
    private Set<String> getAllActiveJobGroups() {
        Set<String> groups = new HashSet<>();
        try {
            List<JobDef> allJobs = jobDefService.list();
            for (JobDef job : allJobs) {
                if (job.getJobGroup() != null && job.getIsActive()) {
                    groups.add(job.getJobGroup());
                }
            }
        } catch (Exception e) {
            logger.error("获取活跃作业组失败", e);
        }
        return groups;
    }
    
    /**
     * 初始化单个作业组的数据
     */
    private void initializeJobGroupData(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            
            if (jobGroupDataCache.containsKey(groupKey)) {
                logger.debug("作业组 {} 数据已缓存，跳过加载", groupKey);
                return;
            }
            
            // 从JobDefService获取作业组数据（初始化时直接获取）
            List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
            
            if (groupJobs != null && !groupJobs.isEmpty()) {
                // 缓存作业组数据
                jobGroupDataCache.put(groupKey, new JobGroupData(groupName, groupJobs));
                
                // 缓存每个作业的定义
                for (JobDef job : groupJobs) {
                    String jobKey = CacheKeyUtils.generateJobKey(job.getJobCode(), batchNo);
                    jobDefCache.put(jobKey, new JobData(job));
                }
                
                // 添加到活跃作业组集合
                activeJobGroups.add(groupKey);
                
                logger.debug("作业组 {} 数据初始化完成，包含 {} 个作业", groupKey, groupJobs.size());
            }
        } catch (Exception e) {
            logger.error("初始化作业组 {} 数据失败", groupName, e);
        }
    }

    public void resetGroupCancelledStatus(String groupName,String batchNo){

    }
    /**
     * 初始化所有作业的状态
     */
    /*
    private void initializeAllJobStatus() {
        try {
            // 清理所有过期的取消标志
        cleanupAllExpiredCancelFlags();
            
            // 初始化作业组状态
            for (String groupName : activeJobGroups) {
                initializeGroupStatus(groupName);
            }
            
            // 初始化作业状态
            for (JobData jobData : jobDefCache.values()) {
                if (jobData.getIsActive()) {
                    initializeJobStatus(jobData.getJobCode());
                }
            }
            
            logger.info("所有作业状态初始化完成");
            
        } catch (Exception e) {
            logger.error("初始化作业状态失败", e);
        }

    }
    */
    
    
    
    /**
     * 初始化作业组状态
     */
    /*
    private void initializeGroupStatus(String groupName) {
        // 使用整合缓存设置初始状态
        JobGroupData groupData = jobGroupDataCache.get(groupName);
        if (groupData != null) {
            groupData.updateStatus(ExecutionStatus.PENDING);
        }
        
        // 初始化统计信息
        groupStatistics.putIfAbsent(groupName, new GroupExecutionStatistics());
        groupStatistics.get(groupName).setGroupName(groupName);
        
        logger.debug("作业组 {} 状态初始化完成", groupName);
    }

    */
    
    /**
     * 初始化作业状态（支持批次号）
     */
    private void initializeJobStatus(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            // 使用整合缓存设置初始状态
            JobData jobData = jobDefCache.get(jobKey);
            if (jobData != null) {
                jobData.updateStatus(ExecutionStatus.PENDING);
            }
            
            logger.debug("作业 {} 状态初始化完成", jobKey);
        } catch (Exception e) {
            logger.error("初始化作业状态失败: {} (批次: {})", jobCode, batchNo, e);
        }
    }
    
    /**
     * 初始化作业状态（向后兼容）
     */
    private void initializeJobStatus(String jobCode) {
        // 使用空批次号进行向后兼容
        initializeJobStatus(jobCode, "");
    }
    
    /**
     * 刷新作业组数据（支持批次号）
     */
    public void refreshJobGroupData(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            logger.info("刷新作业组 {} 数据 (批次: {})", groupKey, batchNo);
            
            // 重新从JobDefService获取数据
            List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
            
            if (groupJobs != null && !groupJobs.isEmpty()) {
                // 更新缓存
                jobGroupDataCache.put(groupKey, new JobGroupData(groupName, groupJobs));
                
                // 更新作业定义缓存
                for (JobDef job : groupJobs) {
                    String jobKey = CacheKeyUtils.generateJobKey(job.getJobCode(), batchNo);
                    jobDefCache.put(jobKey, new JobData(job));
                }
                
                // 确保作业组在活跃集合中
                activeJobGroups.add(groupKey);
                
                logger.info("作业组 {} 数据刷新完成，包含 {} 个作业", groupKey, groupJobs.size());
            } else {
                // 如果作业组没有作业，从活跃集合中移除
                activeJobGroups.remove(groupKey);
                jobGroupDataCache.remove(groupKey);
                logger.info("作业组 {} 没有活跃作业，已从活跃集合中移除", groupKey);
            }
            
        } catch (Exception e) {
            logger.error("刷新作业组 {} 数据失败 (批次: {})", groupName, batchNo, e);
        }
    }
    
    /**
     * 刷新作业组数据（向后兼容）
     */
    public void refreshJobGroupData(String groupName) {
        // 使用空批次号进行向后兼容
        refreshJobGroupData(groupName, "");
    }
    
    /**
     * 刷新所有作业组数据
     */
    public void refreshAllJobGroupData() {
        try {
            logger.info("开始刷新所有作业组数据...");
            
            // 获取最新的活跃作业组
            Set<String> latestGroups = getAllActiveJobGroups();
            
            // 刷新每个作业组的数据
            for (String groupName : latestGroups) {
                refreshJobGroupData(groupName);
            }
            
            // 清理不再活跃的作业组
            Set<String> groupsToRemove = new HashSet<>(activeJobGroups);
            groupsToRemove.removeAll(latestGroups);
            
            for (String groupName : groupsToRemove) {
                activeJobGroups.remove(groupName);
                jobGroupDataCache.remove(groupName);
                logger.info("移除不再活跃的作业组: {}", groupName);
            }
            
            logger.info("所有作业组数据刷新完成，当前活跃作业组: {}", activeJobGroups);
            
        } catch (Exception e) {
            logger.error("刷新所有作业组数据失败", e);
        }
    }
    
    /**
     * 获取作业组数据（优先从缓存获取）
     */
    @Override
    public List<JobDef> getJobGroupData(String groupName, String batchNo) {
        String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
        
        // 首先尝试从缓存获取
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        if (groupData != null) {
            return groupData.getJobs();
        }
        
        // 缓存中没有，从JobDefService获取并缓存
        List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
        if (groupJobs != null && !groupJobs.isEmpty()) {
            jobGroupDataCache.put(groupKey, new JobGroupData(groupName, groupJobs));
            activeJobGroups.add(groupKey);
            
            // 同时缓存作业定义
            for (JobDef job : groupJobs) {
                String jobKey = CacheKeyUtils.generateJobKey(job.getJobCode(), batchNo);
                jobDefCache.put(jobKey, new JobData(job));
            }
        }
        return groupJobs;
    }
    
    /**
     * 获取作业定义（优先从缓存获取，支持批次号）
     */
    public JobDef getJobDef(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            // 首先尝试从缓存获取
            JobData jobData = jobDefCache.get(jobKey);
            if (jobData != null) {
                return jobData.getJobDef();
            }
            
            // 缓存中没有，从JobDefService获取并缓存
            JobDef jobDef = jobDefService.getJobByCode(jobCode);
            if (jobDef != null) {
                jobDefCache.put(jobKey, new JobData(jobDef));
            }
            return jobDef;
        } catch (Exception e) {
            logger.error("获取作业定义失败: {} (批次: {})", jobCode, batchNo, e);
            return null;
        }
    }
    
    /**
     * 获取作业定义（向后兼容）
     */
    public JobDef getJobDef(String jobCode) {
        // 使用空批次号进行向后兼容
        return getJobDef(jobCode, "");
    }
    
    /**
     * 获取所有活跃的作业组名称
     */
    public Set<String> getActiveJobGroups() {
        return new HashSet<>(activeJobGroups);
    }
    
    /**
     * 检查作业组是否存在
     */
    public boolean isJobGroupExists(String groupName) {
        return activeJobGroups.contains(groupName);
    }
    
    /**
     * 检查作业是否存在（支持批次号）
     */
    public boolean isJobExists(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            return jobDefCache.containsKey(jobKey);
        } catch (Exception e) {
            logger.error("检查作业是否存在失败: {} (批次: {})", jobCode, batchNo, e);
            return false;
        }
    }
    
    /**
     * 检查作业是否存在（向后兼容）
     */
    public boolean isJobExists(String jobCode) {
        // 使用空批次号进行向后兼容
        return isJobExists(jobCode, "");
    }
    

    
    /**
     * 获取执行器地址
     */
    private String getExecutorAddress() {
        try {
            java.net.InetAddress localHost = java.net.InetAddress.getLocalHost();
            return localHost.getHostAddress() + ":8081";
        } catch (Exception e) {
            logger.warn("获取执行器地址失败", e);
            return "unknown:8081";
        }
    }
    
    // ==================== 新增任务控制功能 ====================
    
    /**
     * 启动任务
     */
    public Map<String, Object> startJob(Long jobId, JobStartRequest request) {
        try {
            logger.info("启动任务: jobId={}", jobId);
            
            // 检查任务是否存在
            JobDef jobDef = jobDefService.getById(jobId);
            if (jobDef == null) {
                throw new RuntimeException("任务不存在");
            }
            if(!jobDef.getIsActive()){
                throw new RuntimeException("任务禁用");
            }
            // 生成批次号
            String batchNo = Utils.generateBatchNo(jobId);
            // 检查任务状态是否允许启动
            /*
            if (!canExecuteAction(jobId, batchNo,"start")) {
                throw new RuntimeException("任务状态不允许启动");
            }
            */

            

            
            // 清除相关标志（使用整合缓存）
            String jobKey = CacheKeyUtils.generateJobKey(jobDef.getJobCode(), batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            if (jobData != null) {
                jobData.updateStatus(ExecutionStatus.RUNNING);
            }
            
            // 发布启动事件
            eventPublisher.publishEvent(new JobControlEvent.JobStartedEvent(jobDef.getJobCode(), batchNo));
            
            // 发布作业执行注册事件
            eventPublisher.publishEvent(new JobControlEvent.JobExecutionRegisteredEvent(
                jobDef.getJobCode(), 
                batchNo, 
                jobDef.getJobGroup()
            ));
            
            // 1. 创建执行日志记录（状态：PENDING）
            JobExecutionLog executionLog = new JobExecutionLog();
            executionLog.setJobCode(jobDef.getJobCode());
            executionLog.setBatchNo(batchNo);
            executionLog.setStatus("PENDING");  // 使用大写状态值
            executionLog.setStartTime(new Date());
            executionLog.setExecutorProc(jobDef.getJobType());
            executionLog.setExecutorAddress(getExecutorAddress());
            
            // 保存初始记录
            jobExecutionLogService.save(executionLog);
            logger.info("创建执行日志记录: logId={}, status=PENDING", executionLog.getLogId());
            
            // 2. 检查作业当前状态，如果是暂停状态则先恢复
            ExecutionStatus currentStatus = getJobStatus(jobDef.getJobCode(), batchNo);
            if (currentStatus == ExecutionStatus.PAUSED) {
                logger.info("作业 {} 当前处于暂停状态，先恢复再执行", jobDef.getJobCode());
                // 恢复作业状态
                jobData.updateStatus(ExecutionStatus.RUNNING);
                // 更新执行日志状态为RUNNING
                jobExecutionLogService.updateStatus(executionLog.getLogId(), "RUNNING", "恢复暂停的作业并开始执行");
            } else {
                // 更新状态为RUNNING
                jobExecutionLogService.updateStatus(executionLog.getLogId(), "RUNNING", "开始执行任务");
            }
            logger.info("更新执行状态为RUNNING: logId={}", executionLog.getLogId());
            
            // 异步执行任务（避免阻塞调用方）
            controlExecutor.submit(() -> {
                try {
                    logger.info("开始执行任务: {} ({})", jobDef.getJobName(), jobDef.getJobCode());
                    
                    // 实际执行任务
                    JobExecutionResult result = workerHandler.executeJob(jobDef, batchNo);
                    
                    // 3. 根据执行结果更新状态
                    String finalStatus = result.isSuccess() ? "SUCCESS" : "FAILED";  // 使用大写状态值
                    jobExecutionLogService.updateStatus(executionLog.getLogId(), finalStatus, 
                        result.isSuccess() ? "任务执行成功" : result.getErrorMessage());
                    
                    // 4. 更新执行时间和耗时
                    jobExecutionLogService.updateExecutionTime(executionLog.getLogId(), 
                        result.getEndTime(), result.getDuration());
                    
                    // 根据执行结果更新任务状态
                    if (result.isSuccess()) {
                        // 使用整合缓存更新状态
                        if (jobData != null) {
                            jobData.updateStatus(ExecutionStatus.COMPLETED);
                        }
                        completeJob(jobDef.getJobCode(),batchNo,true);
                        logger.info("任务执行成功: {} ({})", jobDef.getJobName(), jobDef.getJobCode());
                    } else {
                        // 使用整合缓存更新状态
                        if (jobData != null) {
                            jobData.updateStatus(ExecutionStatus.FAILED);
                        }
                        completeJob(jobDef.getJobCode(),batchNo,false);
                        logger.error("任务执行失败: {} ({}), 错误: {}", 
                                   jobDef.getJobName(), jobDef.getJobCode(), result.getErrorMessage());
                    }
                    
                    // 发布作业执行完成事件
                    eventPublisher.publishEvent(new JobControlEvent.JobExecutionCompletedEvent(jobDef.getJobCode(), batchNo));
                    
                } catch (Exception e) {
                    logger.error("任务执行异常: {} ({})", jobDef.getJobName(), jobDef.getJobCode(), e);
                    // 5. 异常情况更新状态
                    jobExecutionLogService.updateStatus(executionLog.getLogId(), "FAILED",  // 使用大写状态值
                        "任务执行异常: " + e.getMessage());
                    // 更新任务状态为失败（使用整合缓存）
                    if (jobData != null) {
                        jobData.updateStatus(ExecutionStatus.FAILED);
                    }
                    
                    // 异常情况下也要发布作业执行完成事件
                    eventPublisher.publishEvent(new JobControlEvent.JobExecutionCompletedEvent(jobDef.getJobCode(), batchNo));
                }
            });
            
            // 构建返回结果
            Map<String, Object> result = new HashMap<>();
            result.put("batchNo", batchNo);
            result.put("executionId", "exec_" + System.currentTimeMillis());
            
            logger.info("启动任务成功, jobId: {}, batchNo: {}", jobId, batchNo);
            return result;
            
        } catch (Exception e) {
            logger.error("启动任务失败, jobId: {}", jobId, e);
            throw new RuntimeException("启动任务失败: " + e.getMessage());
        }
    }
    
    /**
     * 批量启动任务
     */
    public Map<String, Object> batchStartJobs(BatchJobRequest request) {
        try {
            List<Long> jobIds = request.getJobIds();
            if (jobIds == null || jobIds.isEmpty()) {
                throw new RuntimeException("任务ID列表不能为空");
            }
            
            List<Map<String, Object>> results = new ArrayList<>();
            int successCount = 0;
            int failedCount = 0;
            
            for (Long jobId : jobIds) {
                try {
                    JobStartRequest startRequest = new JobStartRequest(request.getParams());
                    Map<String, Object> result = startJob(jobId, startRequest);
                    
                    Map<String, Object> jobResult = new HashMap<>();
                    jobResult.put("jobId", jobId);
                    jobResult.put("success", true);
                    jobResult.put("batchNo", result.get("batchNo"));
                    results.add(jobResult);
                    successCount++;
                    
                } catch (Exception e) {
                    logger.error("批量启动任务失败, jobId: {}", jobId, e);
                    
                    Map<String, Object> jobResult = new HashMap<>();
                    jobResult.put("jobId", jobId);
                    jobResult.put("success", false);
                    jobResult.put("error", e.getMessage());
                    results.add(jobResult);
                    failedCount++;
                }
            }
            
            Map<String, Object> batchResult = new HashMap<>();
            batchResult.put("successCount", successCount);
            batchResult.put("failedCount", failedCount);
            batchResult.put("results", results);
            
            logger.info("批量启动任务完成, 成功: {}, 失败: {}", successCount, failedCount);
            return batchResult;
            
        } catch (Exception e) {
            logger.error("批量启动任务失败", e);
            throw new RuntimeException("批量启动任务失败: " + e.getMessage());
        }
    }
    
    /**
     * 批量停止任务
     */

    public Map<String, Object> batchStopJobs(BatchJobRequest request) {
        try {
            List<Long> jobIds = request.getJobIds();
            if (jobIds == null || jobIds.isEmpty()) {
                throw new RuntimeException("任务ID列表不能为空");
            }
            
            int successCount = 0;
            int failedCount = 0;
            
            for (Long jobId : jobIds) {
                try {
                    JobExecutionLog jobLog = jobExecutionLogService.getById(jobId);
                    
                    if (jobLog != null) {
                        boolean result = stopJob(jobLog.getJobCode(),jobLog.getBatchNo());
                        if (result) {
                            successCount++;
                        } else {
                            failedCount++;
                        }
                    } else {
                        failedCount++;
                    }
                } catch (Exception e) {
                    logger.error("批量停止任务失败, jobId: {}", jobId, e);
                    failedCount++;
                }
            }
            
            Map<String, Object> batchResult = new HashMap<>();
            batchResult.put("successCount", successCount);
            batchResult.put("failedCount", failedCount);
            
            logger.info("批量停止任务完成, 成功: {}, 失败: {}", successCount, failedCount);
            return batchResult;
            
        } catch (Exception e) {
            logger.error("批量停止任务失败", e);
            throw new RuntimeException("批量停止任务失败: " + e.getMessage());
        }
    }
    
    /**
     * 检查任务状态是否允许操作
     */
    public boolean canExecuteAction(Long jobId,  String batchNo,String action) {
        try {
            JobDef jobDef = jobDefService.getById(jobId);
            if (jobDef == null) {
                return false;
            }
            
            // 使用完善的getJobStatus方法获取真实状态
            ExecutionStatus currentStatus = getJobStatus(jobDef.getJobCode(),batchNo);
            
            // 根据任务状态和操作类型判断是否允许执行
            switch (action.toLowerCase()) {
                case "start":
                    return currentStatus == ExecutionStatus.PENDING || currentStatus == ExecutionStatus.STOPPED || currentStatus == ExecutionStatus.FAILED ||
                           currentStatus == ExecutionStatus.PAUSED || currentStatus == ExecutionStatus.CANCELLED ||
                            currentStatus== ExecutionStatus.COMPLETED;
                case "stop":
                    return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PAUSED;
                case "pause":
                    return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PENDING;  // 允许运行中和等待中的任务暂停
                case "resume":
                    return currentStatus == ExecutionStatus.PAUSED;
                case "cancel":
                    return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PAUSED;
                default:
                    return false;
            }
            
        } catch (Exception e) {
            logger.error("检查任务操作权限失败, jobId: {}, action: {}", jobId, action, e);
            return false;
        }
    }
    

    
    /**
     * 将数据库状态映射到执行状态
     */
    private ExecutionStatus mapDatabaseStatusToExecutionStatus(String dbStatus) {
        if (dbStatus == null) {
            return ExecutionStatus.PENDING;
        }
        
        switch (dbStatus.toLowerCase()) {
            case "active":
                return ExecutionStatus.PENDING;  // active 表示可用，不是运行中
            case "inactive":
                return ExecutionStatus.STOPPED;
            case "running":
                return ExecutionStatus.RUNNING;
            case "paused":
                return ExecutionStatus.PAUSED;
            case "stopped":
                return ExecutionStatus.STOPPED;
            case "completed":
                return ExecutionStatus.COMPLETED;
            case "failed":
                return ExecutionStatus.FAILED;
            case "cancelled":
                return ExecutionStatus.CANCELLED;
            default:
                return ExecutionStatus.STOPPED;
        }
    }
    
    /**
     * 将执行日志状态映射到执行状态
     */
    private ExecutionStatus mapExecutionLogStatusToExecutionStatus(String logStatus) {
        if (logStatus == null) {
            return ExecutionStatus.PENDING;
        }
        
        switch (logStatus.toUpperCase()) {  // 转换为大写进行比较
            case "RUNNING":
                return ExecutionStatus.RUNNING;
            case "PAUSED":
                return ExecutionStatus.PAUSED;
            case "STOPPED":
                return ExecutionStatus.STOPPED;
            case "COMPLETED":
                return ExecutionStatus.COMPLETED;
            case "SUCCESS":
                return ExecutionStatus.COMPLETED;
            case "FAILED":
                return ExecutionStatus.FAILED;
            case "CANCELLED":
                return ExecutionStatus.CANCELLED;
            case "PENDING":
                return ExecutionStatus.PENDING;  // 修复：正确映射为PENDING状态
            default:
                return ExecutionStatus.STOPPED;
        }
    }
    
    // ==================== 现有作业组控制功能 ====================

    
    @Override
    public boolean cancelAllJobGroups() {
        try {
            logger.info("开始取消所有作业组");
            
            // 获取所有活跃的作业组
            Set<String> activeGroups = new HashSet<>();
            
            // 从整合缓存中获取所有作业组
            activeGroups.addAll(jobGroupDataCache.keySet());
            
            if (activeGroups.isEmpty()) {
                logger.info("没有找到活跃的作业组");
                return true;
            }
            
            logger.info("找到 {} 个活跃作业组: {}", activeGroups.size(), activeGroups);
            
            // 取消所有作业组
            int successCount = 0;
            int totalCount = activeGroups.size();
            
            for (String groupkey : activeGroups) {
                try {
                    String groupName = CacheKeyUtils.extractGroupName(groupkey);
                    String batchNo = CacheKeyUtils.extractBatchNo(groupkey);
                    boolean cancelled = cancelJobGroup(groupName,batchNo);
                    if (cancelled) {
                        successCount++;
                        logger.info("成功取消作业组: {}", groupName);
                    } else {
                        logger.warn("取消作业组失败: {}", groupName);
                    }
                } catch (Exception e) {
                    logger.error("取消作业组 {} 时发生异常", groupkey, e);
                }
            }
            
            logger.info("批量取消作业组完成: 总计={}, 成功={}, 失败={}", 
                       totalCount, successCount, totalCount - successCount);
            
            return successCount > 0;
            
        } catch (Exception e) {
            logger.error("批量取消所有作业组失败", e);
            return false;
        }
    }
    
    @Override
    public boolean cancelBatchJobs(String batchNo) {
        try {
            logger.info("开始取消批次 {} 的所有作业", batchNo);
            
            if (batchNo == null || batchNo.trim().isEmpty()) {
                logger.warn("批次号为空，无法取消");
                return false;
            }
            
            // 获取批次中的所有作业
            List<String> batchJobCodes = getBatchJobCodes(batchNo);
            if (batchJobCodes.isEmpty()) {
                logger.info("批次 {} 中没有找到作业", batchNo);
                return true;
            }
            
            logger.info("批次 {} 中找到 {} 个作业: {}", batchNo, batchJobCodes.size(), batchJobCodes);
            
            // 取消所有作业
            int successCount = 0;
            int totalCount = batchJobCodes.size();
            
            for (String jobCode : batchJobCodes) {
                try {
                    boolean cancelled = cancelJob(jobCode,batchNo);
                    if (cancelled) {
                        successCount++;
                        logger.info("成功取消作业: {} (批次: {})", jobCode, batchNo);
                    } else {
                        logger.warn("取消作业失败: {} (批次: {})", jobCode, batchNo);
                    }
                } catch (Exception e) {
                    logger.error("取消作业 {} 时发生异常", jobCode, e);
                }
            }
            
            logger.info("批次 {} 取消完成: 总计={}, 成功={}, 失败={}", 
                       batchNo, totalCount, successCount, totalCount - successCount);
            
            return successCount > 0;
            
        } catch (Exception e) {
            logger.error("取消批次 {} 失败", batchNo, e);
            return false;
        }
    }
    


    
    @Override
    public ExecutionStatus getJobLatestExecutionStatus(String jobCode) {
        try {
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectByJobCode(jobCode);
            if (!logs.isEmpty()) {
                JobExecutionLog latestLog = logs.get(0); // 按start_time DESC排序，第一个是最新的
                return mapExecutionLogStatusToExecutionStatus(latestLog.getStatus());
            }
        } catch (Exception e) {
            logger.warn("查询任务 {} 最新执行日志状态失败", jobCode, e);
        }
        
        // 如果获取不到，返回默认状态
        return ExecutionStatus.STOPPED;
    }
    
    @Override
    public Map<String, ExecutionStatus> getAllGroupStatus() {
        // 从整合缓存获取所有作业组状态
        Map<String, ExecutionStatus> statuses = new HashMap<>();
        for (Map.Entry<String, JobGroupData> entry : jobGroupDataCache.entrySet()) {
            statuses.put(entry.getKey(), entry.getValue().getStatus());
        }
        return statuses;
    }
    
    @Override
    public Map<String, ExecutionStatus> getAllJobStatus() {
        // 从整合缓存获取所有作业状态
        Map<String, ExecutionStatus> statuses = new HashMap<>();
        for (Map.Entry<String, JobData> entry : jobDefCache.entrySet()) {
            statuses.put(entry.getKey(), entry.getValue().getStatus());
        }
        return statuses;
    }
    
    @Override
    public GroupExecutionStatistics getGroupStatistics(String groupName) {
        return groupStatistics.computeIfAbsent(groupName, k -> new GroupExecutionStatistics());
    }
    
    @Override
    public SystemExecutionStatistics getSystemStatistics() {
        updateSystemStatistics();
        return systemStatistics;
    }
    @Override
    public boolean canExecuteGroupAction(String groupKey, ControlAction action) {
        String groupName = CacheKeyUtils.extractGroupName(groupKey);
        String batchNo = CacheKeyUtils.extractBatchNo(groupKey);
        // 添加调试日志
        boolean isStopped = isGroupStopped(groupName,batchNo);
        boolean isPaused = isGroupPaused(groupName,batchNo);
        boolean isCancelled = isGroupCancelled(groupName,batchNo);
        ExecutionStatus currentStatus = getGroupStatus(groupName,batchNo);

        logger.debug("检查作业组 {} 操作权限: 动作={}, 停止={}, 暂停={}, 取消={}, 状态={}",
                groupName, action, isStopped, isPaused, isCancelled, currentStatus);

        // 修复：优先检查标志状态，然后检查状态映射
        switch (action) {
            case PAUSE:
                // 只有运行中的作业组才能暂停
                boolean canPause = !isStopped && !isPaused && !isCancelled;
                logger.debug("作业组 {} 暂停权限: {}", groupName, canPause);
                return canPause;

            case RESUME:
                // 只有暂停的作业组才能恢复
                boolean canResume = isPaused;
                logger.debug("作业组 {} 恢复权限: {}", groupName, canResume);
                return canResume;

            case STOP:
                // 只要不是已停止的作业组都可以停止
                boolean canStop = !isStopped;
                logger.debug("作业组 {} 停止权限: {}", groupName, canStop);
                return canStop;

            case RESTART:
                // 已停止或失败的作业组可以重启
                boolean canRestart = currentStatus == ExecutionStatus.STOPPED || currentStatus == ExecutionStatus.FAILED;
                logger.debug("作业组 {} 重启权限: {}", groupName, canRestart);
                return canRestart;

            case CANCEL:
                // 修复：允许已取消的组重新取消（用于重置状态）
                boolean canCancel = true; // 所有组都可以取消
                logger.debug("作业组 {} 取消权限: {}", groupName, canCancel);
                return canCancel;

            default:
                logger.debug("作业组 {} 未知操作: {}", groupName, action);
                    return false;
                }
            }
        @Override
    public boolean canExecuteGroupAction(String groupName,String batchNo, ControlAction action) {
        // 添加调试日志
        boolean isStopped = isGroupStopped(groupName,batchNo);
        boolean isPaused = isGroupPaused(groupName,batchNo);
        boolean isCancelled = isGroupCancelled(groupName,batchNo);
        ExecutionStatus currentStatus = getGroupStatus(groupName,batchNo);
        
        logger.debug("检查作业组 {} 操作权限: 动作={}, 停止={}, 暂停={}, 取消={}, 状态={}", 
                    groupName, action, isStopped, isPaused, isCancelled, currentStatus);
        
        // 修复：优先检查标志状态，然后检查状态映射
        switch (action) {
            case PAUSE:
                // 只有运行中的作业组才能暂停
                boolean canPause = !isStopped && !isPaused && !isCancelled;
                logger.debug("作业组 {} 暂停权限: {}", groupName, canPause);
                return canPause;
                
            case RESUME:
                // 只有暂停的作业组才能恢复
                boolean canResume = isPaused;
                logger.debug("作业组 {} 恢复权限: {}", groupName, canResume);
                return canResume;
                
            case STOP:
                // 只要不是已停止的作业组都可以停止
                boolean canStop = !isStopped;
                logger.debug("作业组 {} 停止权限: {}", groupName, canStop);
                return canStop;
                
            case RESTART:
                // 已停止或失败的作业组可以重启
                boolean canRestart = currentStatus == ExecutionStatus.STOPPED || currentStatus == ExecutionStatus.FAILED;
                logger.debug("作业组 {} 重启权限: {}", groupName, canRestart);
                return canRestart;
                
            case CANCEL:
                // 修复：允许已取消的组重新取消（用于重置状态）
                boolean canCancel = true; // 所有组都可以取消
                logger.debug("作业组 {} 取消权限: {}", groupName, canCancel);
                return canCancel;
                
            default:
                logger.debug("作业组 {} 未知操作: {}", groupName, action);
            return false;
        }
    }
    @Override
    public boolean canExecuteJobAction(String jobKey,ControlAction action) {
        String jobCode = CacheKeyUtils.extractJobCode(jobKey);
        String batchNo = CacheKeyUtils.extractBatchNo(jobKey);
        ExecutionStatus currentStatus = getJobStatus(jobCode,batchNo);

        switch (action) {
            case PAUSE:
                return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PENDING;  // 允许运行中和等待中的任务暂停
            case RESUME:
                return currentStatus == ExecutionStatus.PAUSED;
            case STOP:
                return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PAUSED;
            case RESTART:
                return currentStatus == ExecutionStatus.STOPPED || currentStatus == ExecutionStatus.FAILED ||
                        currentStatus == ExecutionStatus.PAUSED || currentStatus == ExecutionStatus.CANCELLED;
            case CANCEL:
                // 修复：允许取消任何状态的作业，以便组取消操作能够完整执行
                // 当作业组被强制取消时，需要能够取消所有作业
                boolean canCancel = true;
                logger.debug("作业 {} 取消权限检查: 当前状态={}, 允许取消={}", jobCode, currentStatus, canCancel);
                return canCancel;
            default:
                return false;
        }
    }
    @Override
    public boolean canExecuteJobAction(String jobCode, String batchNo,ControlAction action) {
        ExecutionStatus currentStatus = getJobStatus(jobCode,batchNo);
        
        switch (action) {
            case PAUSE:
                return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PENDING;  // 允许运行中和等待中的任务暂停
            case RESUME:
                return currentStatus == ExecutionStatus.PAUSED;
            case STOP:
                return currentStatus == ExecutionStatus.RUNNING || currentStatus == ExecutionStatus.PAUSED;
            case RESTART:
                return currentStatus == ExecutionStatus.STOPPED || currentStatus == ExecutionStatus.FAILED || 
                       currentStatus == ExecutionStatus.PAUSED || currentStatus == ExecutionStatus.CANCELLED;
            case CANCEL:
                // 修复：允许取消任何状态的作业，以便组取消操作能够完整执行
                // 当作业组被强制取消时，需要能够取消所有作业
                boolean canCancel = true;
                logger.debug("作业 {} 取消权限检查: 当前状态={}, 允许取消={}", jobCode, currentStatus, canCancel);
                return canCancel;
            default:
                return false;
        }
    }
    

    
    @Override
    public void waitForGroupPause(String groupName,String batchNo) {
        while (isGroupPaused(groupName,batchNo)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    

    
    @Override
    public void initializeGroup(String groupName,String batchNo) {
        try {

            String groupKey = CacheKeyUtils.generateJobKey(groupName, batchNo);
            // 重新初始化统计信息
            groupStatistics.computeIfAbsent(groupName, k -> {
                GroupExecutionStatistics stats = new GroupExecutionStatistics();
                stats.setGroupName(groupName);
                stats.setBatchNo(batchNo);
                stats.setStartTime(System.currentTimeMillis());
                return stats;
            });
            
            // 修复：获取组内所有作业，确保它们的状态也被重置为运行状态
            List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
            if (groupJobs != null) {
                for (JobDef jobDef : groupJobs) {
                    if (jobDef.getIsActive()) {
                        // 重置每个作业的状态为运行中
                        clearJobFlags(jobDef.getJobCode(),batchNo);
                        logger.debug("重置作业 {} 状态为 RUNNING", jobDef.getJobCode());
                    }
                }
            }
            
            logger.info("初始化作业组: {}，状态设置为 RUNNING，组内作业状态已重置，取消标志已清理", groupName);
            
        } catch (Exception e) {
            logger.error("初始化作业组 {} 失败", groupName, e);
        }
    }
    
    @Override
    public void initializeJob(String jobCode,String batchNo) {
        // 清除作业相关的标志
        clearJobFlags(jobCode,batchNo);
        
        // 额外确保取消标志被清理（双重保险）
        // 现在由整合缓存管理，不需要手动清理
        
        // 重置状态为运行中
        // 现在由整合缓存管理，不需要手动设置
        
        logger.info("初始化作业: {}，状态设置为 RUNNING，取消标志已清理", jobCode);
    }
    
    @Override
    public void completeJob(String jobCode,String batchNo, boolean success) {
        try {
        ExecutionStatus status = success ? ExecutionStatus.COMPLETED : ExecutionStatus.FAILED;
            logger.info("作业 {} 执行完成，状态: {}", jobCode, status);
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            // 1. 从作业缓存中移除作业
            JobData jobData = jobDefCache.remove(jobKey);
            if (jobData == null) {
                logger.warn("作业 {} 在缓存中不存在，无法完成", jobCode);
                return;
            }
            
            // 2. 获取作业所属的作业组
            String groupName = jobData.getJobGroup();
            if (groupName == null) {
                logger.warn("作业 {} 没有关联的作业组", jobCode);
                return;
            }
            String groupKey = CacheKeyUtils.generateJobKey(groupName, batchNo);
            // 3. 从作业组中移除该作业
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            if (groupData != null) {
                // 🎯 使用线程安全的方式从作业组中移除作业
                boolean removed = groupData.removeJob(jobCode);
                if (removed) {
                    logger.debug("从作业组 {} 中移除作业: {}", groupName, jobCode);
                }
                
                // 4. 检查作业组是否为空（没有更多作业）
                if (groupData.isEmpty()) {
                    logger.info("作业组 {} 中所有作业已完成，从缓存中移除作业组", groupName);
                    
                    // 从作业组缓存中移除
                    jobGroupDataCache.remove(groupKey);
                    
                    // 从活跃作业组集合中移除
                    activeJobGroups.remove(groupKey);
                    
                    // 清理相关资源
                    groupLocks.remove(groupKey);
                    groupStatistics.remove(groupKey);
                    
                    logger.info("作业组 {} 已从所有缓存中清理完成", groupName);
                } else {
                    logger.debug("作业组 {} 中还有 {} 个作业，保留作业组", 
                               groupName, groupData.getJobCount());
                }
            } else {
                logger.warn("作业组 {} 在缓存中不存在", groupName);
            }
            
            // 5. 清理作业相关资源
            jobLocks.remove(jobKey);
            
            logger.info("作业 {} 完成处理完成，已从缓存中移除", jobCode);
                    
                } catch (Exception e) {
            logger.error("完成作业 {} 时发生异常", jobCode, e);
        }
    }
    
    @Override
    public void completeGroup(String groupName,String batchNo, boolean success) {
        try {
            logger.info("作业组 {} 执行完成，成功: {}", groupName, success);
            String groupKey = CacheKeyUtils.generateJobKey(groupName, batchNo);
            // 修复：检查作业组是否被取消，如果被取消则不自动重置
            if (isGroupCancelled(groupKey,batchNo)) {
                logger.info("作业组 {} 已被取消，不执行自动重置，等待取消操作完成", groupName);
                return;
            }
            
            // 更新统计信息
            updateGroupStatistics(groupName,batchNo);
            
            // 设置完成状态
            if (success) {
                // 使用整合缓存更新状态
                JobGroupData groupData = jobGroupDataCache.get(groupKey);
                if (groupData != null) {
                    groupData.updateStatus(ExecutionStatus.COMPLETED);
                }
                logger.info("作业组 {} 执行成功，状态设置为已完成", groupName);
            } else {
                // 使用整合缓存更新状态
                JobGroupData groupData = jobGroupDataCache.get(groupKey);
                if (groupData != null) {
                    groupData.updateStatus(ExecutionStatus.FAILED);
                }
                logger.error("作业组 {} 执行失败，状态设置为失败", groupName);
            }
            
            // 自动重置为初始状态，为下次执行做准备
            logger.info("作业组 {} 执行完成，自动重置为初始状态，为下次执行做准备", groupName);
            resetGroupStatus(groupName,batchNo);
            
        } catch (Exception e) {
            logger.error("完成作业组 {} 失败", groupName, e);
        }
    }
    
    /**
     * 清除作业组所有标志（使用整合缓存）
     */
    @Override
    public void clearGroupFlags(String groupName,String batchNo) {
        // 使用空批次号进行向后兼容
        clearGroupFlagsInternal(groupName, batchNo);
    }
    
    /**
     * 清除作业组所有标志（内部实现，支持批次号）
     */
    private void clearGroupFlagsInternal(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            logger.info("清除作业组 {} 所有标志，状态重置为初始状态 (STOPPED)", groupKey);
            
            // 使用整合缓存重置状态
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            if (groupData != null) {
                // 重置为初始状态
                groupData.updateStatus(ExecutionStatus.STOPPED);
                logger.debug("作业组 {} 整合缓存状态已重置为 STOPPED", groupKey);
            }
            
            // 初始化统计信息
            groupStatistics.putIfAbsent(groupKey, new GroupExecutionStatistics());
            groupStatistics.get(groupKey).setGroupName(groupName);
            
            logger.info("作业组 {} 状态已重置为初始状态，现在可以重新执行", groupKey);
            
        } catch (Exception e) {
            logger.error("清除作业组 {} 标志失败 (批次: {})", groupName, batchNo, e);
        }
    }
    
    /**
     * 清除作业所有标志（使用整合缓存）
     */
    @Override
    public void clearJobFlags(String jobCode,String batchNo) {
        // 使用空批次号进行向后兼容
        clearJobFlagsInternal(jobCode, batchNo);
    }
    
    /**
     * 清除作业所有标志（内部实现，支持批次号）
     */
    private void clearJobFlagsInternal(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            logger.info("清除作业 {} 所有标志，状态重置为初始状态 (PENDING)", jobKey);
            
            // 使用整合缓存重置状态
            JobData jobData = jobDefCache.get(jobKey);
            if (jobData != null) {
                // 重置为初始状态
                jobData.updateStatus(ExecutionStatus.PENDING);
                logger.debug("作业 {} 整合缓存状态已重置为 PENDING", jobKey);
            }
            
            logger.info("作业 {} 状态已重置为初始状态，现在可以重新执行", jobKey);
            
        } catch (Exception e) {
            logger.error("清除作业 {} 标志失败 (批次: {})", jobCode, batchNo, e);
        }
    }
    
    /**
     * 清除作业组取消状态（使用整合缓存）
     */
    @Override
    public void clearGroupCancelledStatus(String groupName,String batchNo) {
        // 使用空批次号进行向后兼容
        clearGroupCancelledStatusInternal(groupName, batchNo);
    }
    
    /**
     * 清除作业组取消状态（内部实现，支持批次号）
     */
    private void clearGroupCancelledStatusInternal(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            logger.info("清除作业组 {} 取消状态，状态重置为 RUNNING", groupKey);
            
            // 使用整合缓存重置状态
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            if (groupData != null) {
                // 重置为运行状态
                groupData.updateStatus(ExecutionStatus.RUNNING);
                logger.debug("作业组 {} 整合缓存状态已重置为 RUNNING", groupKey);
            }
            
            // 状态映射已由整合缓存管理
            
            logger.info("作业组 {} 取消状态已重置，现在可以重新执行", groupKey);
            
        } catch (Exception e) {
            logger.error("清除作业组 {} 取消状态失败 (批次: {})", groupName, batchNo, e);
        }
    }
    
    /**
     * 清除作业取消状态（使用整合缓存）
     */
    @Override
    public void clearJobCancelledStatus(String jobCode,String batchNo) {
        // 使用空批次号进行向后兼容
        clearJobCancelledStatusInternal(jobCode, batchNo);
    }
    
    /**
     * 清除作业取消状态（内部实现，支持批次号）
     */
    private void clearJobCancelledStatusInternal(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            logger.info("清除作业 {} 取消状态，状态重置为 RUNNING", jobKey);
            
            // 使用整合缓存重置状态
            JobData jobData = jobDefCache.get(jobKey);
            if (jobData != null) {
                // 重置为运行状态
                jobData.updateStatus(ExecutionStatus.RUNNING);
                logger.debug("作业 {} 整合缓存状态已重置为 RUNNING", jobKey);
            }
            
            // 状态映射已由整合缓存管理
            
            logger.info("作业 {} 取消状态已重置，现在可以重新执行", jobKey);
            
        } catch (Exception e) {
            logger.error("清除作业 {} 取消状态失败 (批次: {})", jobCode, batchNo, e);
        }
    }
    
    @Override
    public void resetGroupStatus(String groupName,String batchNo) {
        try {
            logger.info("重置作业组 {} 状态为初始状态", groupName);
            
            // 修复：检查作业组是否被取消，如果被取消则不执行重置
            if (isGroupCancelled(groupName,batchNo)) {
                logger.info("作业组 {} 已被取消，不执行自动重置，等待取消操作完成", groupName);
                return;
            }
            
            // 清除所有标志
            clearGroupFlags(groupName,batchNo);
            
            // 额外确保取消标志被清理（双重保险）
            // 现在由整合缓存管理，不需要手动清理
            
            // 重置状态为初始状态（STOPPED，表示可以开始执行）
            // 现在由整合缓存管理，不需要手动设置
            
            // 重置统计信息
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            resetGroupStatistics(groupKey);
            
            // 修复：获取组内所有作业，确保它们的状态也被重置为初始状态
            List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
            if (groupJobs != null) {
                for (JobDef jobDef : groupJobs) {
                    if (jobDef.getJobCode() != null && jobDef.getIsActive()) {
                        // 重置每个作业的状态为初始状态（STOPPED）
                        // 现在由整合缓存管理，不需要手动设置
                        // 清除作业的标志

                        clearJobFlags(jobDef.getJobCode(),batchNo);
                        logger.debug("重置作业 {} 状态为初始状态 (STOPPED)", jobDef.getJobCode());
                    }
                }
            }
            
            logger.info("作业组 {} 状态已重置为初始状态，取消标志已清理，现在可以重新执行", groupName);
            
        } catch (Exception e) {
            logger.error("重置作业组 {} 状态失败", groupName, e);
            throw new RuntimeException("重置作业组状态失败: " + e.getMessage());
        }
    }
    
    @Override
    public void resetGroupStatistics(String groupKey) {
        groupStatistics.remove(groupKey);
        logger.info("重置作业组 {} 统计信息", groupKey);
    }
    /*
    @Override
    public void updateGroupStatus(String groupName,String batchNo, String status) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            ExecutionStatus executionStatus = ExecutionStatus.valueOf(status.toUpperCase());
            
            // 使用整合缓存更新状态
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            if (groupData != null) {
                groupData.updateStatus(executionStatus);
            }
            
            // 修复：同步更新标志状态
            // 现在由整合缓存管理，不需要手动同步
            
            logger.info("更新作业组 {} 状态为: {}，相关标志已同步", groupName, status);
        } catch (IllegalArgumentException e) {
            logger.error("无效的状态值: {}", status);
        }
    }
    */

    
   
   
    private void updateGroupStatistics(String groupName,String batchNo) {
        String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
        GroupExecutionStatistics stats = groupStatistics.computeIfAbsent(groupKey,
            k -> new GroupExecutionStatistics());
        
        try {
            //应该是执行日志获取统计？
            List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
            if (groupJobs != null) {
                stats.setTotalJobs(groupJobs.size());
                
                int completed = 0, failed = 0, paused = 0, stopped = 0, cancelled = 0;
                
                for (JobDef jobDef : groupJobs) {
                    ExecutionStatus status = getJobStatus(jobDef.getJobCode(),batchNo);
                    switch (status) {
                        case COMPLETED: completed++; break;
                        case FAILED: failed++; break;
                        case PAUSED: paused++; break;
                        case STOPPED: stopped++; break;
                        case CANCELLED: cancelled++; break;
                    }
                }
                
                stats.setCompletedJobs(completed);
                stats.setFailedJobs(failed);
                stats.setPausedJobs(paused);
                stats.setStoppedJobs(stopped);
                stats.setCancelledJobs(cancelled);
                
                if (stats.getTotalJobs() > 0) {
                    stats.setProgress((double) (completed + failed) / stats.getTotalJobs() * 100);
                }
            }
        } catch (Exception e) {
            logger.error("更新作业组 {} 统计信息失败", groupName, e);
        }
    }
    
    /**
     * 更新单个作业统计信息
     */
    private void updateJobStatistics(String jobCode,String batchNo) {
        try {
            // 获取作业所属的作业组
            JobDef jobDef = jobDefService.getJobByCode(jobCode);
            if (jobDef != null && jobDef.getJobGroup() != null) {
                // 更新作业组统计信息
                updateGroupStatistics(jobDef.getJobGroup(),batchNo);
            }
            
            logger.debug("更新作业 {} 统计信息", jobCode);
            
        } catch (Exception e) {
            logger.error("更新作业 {} 统计信息失败", jobCode, e);
        }
    }
    
    private void updateSystemStatistics() {
        // 统计作业组
        systemStatistics.setTotalGroups(jobGroupDataCache.size());
        systemStatistics.setActiveGroups((int) jobGroupDataCache.values().stream()
            .filter(groupData -> groupData.getStatus() == ExecutionStatus.RUNNING).count());
        systemStatistics.setPausedGroups((int) jobGroupDataCache.values().stream()
            .filter(groupData -> groupData.getStatus() == ExecutionStatus.PAUSED).count());
        systemStatistics.setStoppedGroups((int) jobGroupDataCache.values().stream()
            .filter(groupData -> groupData.getStatus() == ExecutionStatus.STOPPED).count());
        systemStatistics.setCancelledGroups((int) jobGroupDataCache.values().stream()
            .filter(groupData -> groupData.getStatus() == ExecutionStatus.CANCELLED).count());
        
        // 统计作业
        systemStatistics.setTotalJobs(jobDefCache.size());
        systemStatistics.setActiveJobs((int) jobDefCache.values().stream()
            .filter(jobData -> jobData.getStatus() == ExecutionStatus.RUNNING).count());
        systemStatistics.setPausedJobs((int) jobDefCache.values().stream()
            .filter(jobData -> jobData.getStatus() == ExecutionStatus.PAUSED).count());
        systemStatistics.setStoppedJobs((int) jobDefCache.values().stream()
            .filter(jobData -> jobData.getStatus() == ExecutionStatus.STOPPED).count());
        systemStatistics.setCancelledJobs((int) jobDefCache.values().stream()
            .filter(jobData -> jobData.getStatus() == ExecutionStatus.CANCELLED).count());
        
        // 计算整体进度
        if (systemStatistics.getTotalJobs() > 0) {
            double progress = (double) systemStatistics.getActiveJobs() / systemStatistics.getTotalJobs() * 100;
            systemStatistics.setOverallProgress(progress);
        }
    }


    
    /**
     * 获取指定批次号的所有作业代码
     */
    private List<String> getBatchJobCodes(String batchNo) {
        try {
            // 从执行日志中获取批次的所有作业
            List<JobExecutionLog> batchLogs = jobExecutionLogMapper.selectByJobCode(batchNo);
            if (batchLogs == null || batchLogs.isEmpty()) {
                logger.warn("批次 {} 中没有找到执行日志", batchNo);
                return new ArrayList<>();
            }
            
            // 提取作业代码
            List<String> jobCodes = new ArrayList<>();
            for (JobExecutionLog log : batchLogs) {
                if (log.getJobCode() != null && !jobCodes.contains(log.getJobCode())) {
                    jobCodes.add(log.getJobCode());
                }
            }
            
            logger.info("批次 {} 中找到 {} 个唯一作业: {}", batchNo, jobCodes.size(), jobCodes);
            return jobCodes;
            
        } catch (Exception e) {
            logger.error("获取批次 {} 的作业代码失败", batchNo, e);
            return new ArrayList<>();
        }
    }


    
    /**
     * 清理所有过期的取消标志（系统启动时调用）
     */
    private void cleanupAllExpiredCancelFlags() {
        try {
            logger.info("系统启动，开始清理所有过期的取消标志");
            
            // 清理组级别的过期取消标志
            for (Map.Entry<String, JobGroupData> entry : jobGroupDataCache.entrySet()) {
                String groupName = entry.getKey();
                JobGroupData groupData = entry.getValue();
                if (groupData != null && groupData.isCancelled()) {
                    ExecutionStatus currentStatus = groupData.getStatus();
                    if (currentStatus == ExecutionStatus.COMPLETED) {
                        logger.info("系统启动时清理作业组 {} 过期的取消标志，状态: {}", groupName, currentStatus);
                        
                        // 状态是已完成，重置为初始状态
                        groupData.updateStatus(ExecutionStatus.PENDING);
                        logger.info("系统启动时重置作业组 {} 状态为 STOPPED（初始状态）", groupName);
                    }
                    // 如果状态是 CANCELLED，保持取消状态，不清理标志
                    else if (currentStatus == ExecutionStatus.CANCELLED) {
                        logger.debug("系统启动时作业组 {} 取消标志存在且状态为 CANCELLED，保持取消状态", groupName);
                    }
                }
            }
            
            // 清理作业级别的过期取消标志
            for (Map.Entry<String, JobData> entry : jobDefCache.entrySet()) {
                String jobCode = entry.getKey();
                JobData jobData = entry.getValue();
                if (jobData != null && jobData.isCancelled()) {
                    ExecutionStatus currentStatus = jobData.getStatus();
                    if (currentStatus == ExecutionStatus.COMPLETED) {
                        logger.info("系统启动时清理作业 {} 过期的取消标志，状态: {}", jobCode, currentStatus);
                        
                        // 状态是已完成，重置为初始状态
                        jobData.updateStatus(ExecutionStatus.PENDING);
                        logger.info("系统启动时重置作业 {} 状态为 STOPPED（初始状态）", jobCode);
                    }
                    // 如果状态是 CANCELLED，保持取消状态，不清理标志
                    else if (currentStatus == ExecutionStatus.CANCELLED) {
                        logger.debug("系统启动时作业 {} 取消标志存在且状态为 CANCELLED，保持取消状态", jobCode);
                    }
                }
            }
            
            logger.info("系统启动时清理过期取消标志完成");
            
        } catch (Exception e) {
            logger.error("系统启动时清理过期取消标志失败", e);
        }
    }
    

    
    /**
     * 按需加载批次相关作业数据（外部触发）
     * 当需要处理某个批次时调用
     */
    public void loadBatchJobData(String batchNo) {
        try {
            logger.info("按需加载批次 {} 相关作业数据", batchNo);
            
            // 从执行日志中获取批次相关的作业代码
            List<String> jobCodes = getJobCodesByBatch(batchNo);
            
            if (jobCodes != null && !jobCodes.isEmpty()) {
                for (String jobCode : jobCodes) {
                    loadJobData(jobCode,batchNo);
                }
                
                logger.info("批次 {} 相关作业数据加载完成，共 {} 个作业", batchNo, jobCodes.size());
            } else {
                logger.warn("批次 {} 没有找到相关作业", batchNo);
            }
            
        } catch (Exception e) {
            logger.error("加载批次 {} 作业数据失败", batchNo, e);
        }
    }
    
    /**
     * 按需加载单个作业数据（外部触发）
     * 当需要操作某个具体作业时调用
     */
    public void loadJobData(String jobCode,String batchNo) {
        try {
            logger.debug("按需加载作业 {} 数据", jobCode);
            String jobKey =  CacheKeyUtils.generateJobKey(jobCode,batchNo);
            // 检查是否已经加载过
            if (jobDefCache.containsKey(jobKey)) {
                logger.debug("作业 {} 数据已缓存，跳过加载", jobKey);
                return;
            }
            
            // 从JobDefService获取作业定义
            JobDef jobDef = jobDefService.getJobByCode(jobCode);
            
            if (jobDef != null) {
                // 缓存作业定义
                jobDefCache.put(jobCode, new JobData(jobDef));
                
                // 如果作业有组信息，确保组数据也被加载
                if (jobDef.getJobGroup() != null) {
                    // 对于单个作业加载，使用空批次号
                    loadJobGroupData(jobDef.getJobGroup(), batchNo);
                }
                
                // 初始化作业状态
                initializeJobStatus(jobCode,batchNo);
                
                logger.debug("作业 {} 数据加载完成", jobCode);
            } else {
                logger.warn("作业 {} 不存在", jobCode);
            }
            
        } catch (Exception e) {
            logger.error("加载作业 {} 数据失败", jobCode, e);
        }
    }
    
    /**
     * 按需加载多个作业数据（外部触发）
     * 当需要批量操作多个作业时调用
     */
    public void loadMultipleJobData(List<String> jobCodes) {
        try {
            logger.info("按需加载多个作业数据，共 {} 个作业", jobCodes.size());
            
            for (String jobCode : jobCodes) {
                loadJobData(jobCode,"");
            }
            
            logger.info("多个作业数据加载完成");
            
        } catch (Exception e) {
            logger.error("加载多个作业数据失败", e);
        }
    }

    /**
     * 预加载作业组数据（可选，用于性能优化）
     * 当预期某个作业组即将被使用时调用
     */
    public void preloadJobGroupData(String groupName) {
        try {
            logger.info("预加载作业组 {} 数据", groupName);
            loadJobGroupData(groupName, "");
        } catch (Exception e) {
            logger.error("预加载作业组 {} 数据失败", groupName, e);
        }
    }
    
    /**
     * 清理不再需要的作业组数据（内存管理）
     * 当某个作业组长时间不使用时调用
     */
    public void cleanupJobGroupData(String groupName) {
        try {
            logger.info("清理作业组 {} 数据", groupName);
            
            // 获取组内所有作业代码
            JobGroupData groupData = jobGroupDataCache.get(groupName);
            if (groupData != null) {
                List<JobDef> groupJobs = groupData.getJobs();
                if (groupJobs != null) {
                    for (JobDef job : groupJobs) {
                        jobDefCache.remove(job.getJobCode());
                    }
                }
            }
            
            // 清理组数据
            jobGroupDataCache.remove(groupName);
            activeJobGroups.remove(groupName);
            
            // 清理相关状态（保留控制状态，只清理数据缓存）
            logger.info("作业组 {} 数据清理完成", groupName);
            
        } catch (Exception e) {
            logger.error("清理作业组 {} 数据失败", groupName, e);
        }
    }
    

    
    // ==================== 整合后的状态管理方法 ====================
    
    /**
     * 通过整合缓存获取作业组状态
     */
    public ExecutionStatus getGroupStatusFromCache(String groupName) {
        JobGroupData groupData = jobGroupDataCache.get(groupName);
        return groupData != null ? groupData.getStatus() : ExecutionStatus.STOPPED;
    }
    
    /**
     * 通过整合缓存获取作业状态
     */
    public ExecutionStatus getJobStatusFromCache(String jobCode) {
        JobData jobData = jobDefCache.get(jobCode);
        return jobData != null ? jobData.getStatus() : ExecutionStatus.STOPPED;
    }
    
    /**
     * 通过整合缓存更新作业组状态
     */
    public boolean updateGroupStatusInCache(String groupName, ExecutionStatus newStatus) {
        if (!VALID_STATUSES.contains(newStatus)) {
            logger.warn("无效的作业组状态值: {}", newStatus);
                return false;
        }

        JobGroupData groupData = jobGroupDataCache.get(groupName);
        if (groupData == null) {
            logger.warn("作业组 {} 数据未加载，无法更新状态", groupName);
                return false;
            }
        
        return groupData.updateStatus(newStatus);
    }
    
    /**
     * 通过整合缓存更新作业状态
     */
    public boolean updateJobStatusInCache(String jobCode, ExecutionStatus newStatus) {
        if (!VALID_STATUSES.contains(newStatus)) {
            logger.warn("无效的作业状态值: {}", newStatus);
                return false;
        }
        
        JobData jobData = jobDefCache.get(jobCode);
        if (jobData == null) {
            logger.warn("作业 {} 数据未加载，无法更新状态", jobCode);
            return false;
        }
        
        return jobData.updateStatus(newStatus);
    }
    
    /**
     * 批量更新作业组状态（整合缓存）
     */
    public boolean updateGroupStatesInCache(Map<String, ExecutionStatus> updates) {
        boolean allSuccess = true;
        for (Map.Entry<String, ExecutionStatus> entry : updates.entrySet()) {
            boolean success = updateGroupStatusInCache(entry.getKey(), entry.getValue());
            if (!success) {
                allSuccess = false;
                logger.warn("更新作业组 {} 状态失败: {}", entry.getKey(), entry.getValue());
            }
        }
        return allSuccess;
    }
    
    /**
     * 批量更新作业状态（整合缓存）
     */
    public boolean updateJobStatesInCache(Map<String, ExecutionStatus> updates) {
        boolean allSuccess = true;
        for (Map.Entry<String, ExecutionStatus> entry : updates.entrySet()) {
            boolean success = updateJobStatusInCache(entry.getKey(), entry.getValue());
            if (!success) {
                allSuccess = false;
                logger.warn("更新作业 {} 状态失败: {}", entry.getKey(), entry.getValue());
            }
        }
        return allSuccess;
    }
    
    /**
     * 获取状态快照（整合缓存）
     */
    public StateSnapshot getStateSnapshotFromCache() {
        Map<String, ExecutionStatus> groupStatuses = new HashMap<>();
        Map<String, ExecutionStatus> jobStatuses = new HashMap<>();
        
        for (Map.Entry<String, JobGroupData> entry : jobGroupDataCache.entrySet()) {
            groupStatuses.put(entry.getKey(), entry.getValue().getStatus());
        }
        
        for (Map.Entry<String, JobData> entry : jobDefCache.entrySet()) {
            jobStatuses.put(entry.getKey(), entry.getValue().getStatus());
        }
        
        return new StateSnapshot(groupStatuses, jobStatuses);
    }
    
    /**
     * 状态快照类
     */
    public static class StateSnapshot {
        private final Map<String, ExecutionStatus> groupStatuses;
        private final Map<String, ExecutionStatus> jobStatuses;
        private final long timestamp;
        
        public StateSnapshot(Map<String, ExecutionStatus> groupStatuses, 
                           Map<String, ExecutionStatus> jobStatuses) {
            this.groupStatuses = new HashMap<>(groupStatuses);
            this.jobStatuses = new HashMap<>(jobStatuses);
            this.timestamp = System.currentTimeMillis();
        }
        
        // Getters
        public Map<String, ExecutionStatus> getGroupStatuses() { return groupStatuses; }
        public Map<String, ExecutionStatus> getJobStatuses() { return jobStatuses; }
        public long getTimestamp() { return timestamp; }
    }
    
    // ==================== 新的状态检查方法（替换分散标志） ====================
    
    /**
     * 检查作业组是否暂停（使用整合缓存）
     */
    public boolean isGroupPausedFromCache(String groupKey) {
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        return groupData != null && groupData.isPaused();
    }
    
    /**
     * 检查作业组是否停止（使用整合缓存）
     */
    public boolean isGroupStoppedFromCache(String groupKey) {
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        return groupData != null && groupData.isStopped();
    }
    
    /**
     * 检查作业组是否取消（使用整合缓存）
     */
    public boolean isGroupCancelledFromCache(String groupKey) {
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        return groupData != null && groupData.isCancelled();
    }
    
    /**
     * 检查作业是否暂停（使用整合缓存）
     */
    public boolean isJobPausedFromCache(String jobKey) {
        JobData jobData = jobDefCache.get(jobKey);
        return jobData != null && jobData.isPaused();
    }
    
    /**
     * 检查作业是否停止（使用整合缓存）
     */
    public boolean isJobStoppedFromCache(String jobKey) {
        JobData jobData = jobDefCache.get(jobKey);
        return jobData != null && jobData.isStopped();
    }
    
    /**
     * 检查作业是否取消（使用整合缓存）
     */
    public boolean isJobCancelledFromCache(String jobKey) {
        JobData jobData = jobDefCache.get(jobKey);
        return jobData != null && jobData.isCancelled();
    }
    
    /**
     * 设置作业组暂停状态（使用整合缓存）
     */
    public boolean setGroupPausedFromCache(String groupKey, boolean paused) {
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        if (groupData == null) {
            logger.warn("作业组 {} 数据未加载，无法设置暂停状态", groupKey);
            return false;
        }
        
        if (paused) {
            return groupData.updateStatus(ExecutionStatus.PAUSED);
        } else {
            return groupData.updateStatus(ExecutionStatus.RUNNING);
        }
    }
    
    /**
     * 设置作业组停止状态（使用整合缓存）
     */
    public boolean setGroupStoppedFromCache(String groupKey, boolean stopped) {
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        if (groupData == null) {
            logger.warn("作业组 {} 数据未加载，无法设置停止状态", groupKey);
            return false;
        }
        
        if (stopped) {
            return groupData.updateStatus(ExecutionStatus.STOPPED);
        } else {
            return groupData.updateStatus(ExecutionStatus.RUNNING);
        }
    }
    
    /**
     * 设置作业组取消状态（使用整合缓存）
     */
    public boolean setGroupCancelledFromCache(String groupKey, boolean cancelled) {
        JobGroupData groupData = jobGroupDataCache.get(groupKey);
        if (groupData == null) {
            logger.warn("作业组 {} 数据未加载，无法设置取消状态", groupKey);
            return false;
        }
        
        if (cancelled) {
            return groupData.updateStatus(ExecutionStatus.CANCELLED);
        } else {
            return groupData.updateStatus(ExecutionStatus.STOPPED);
        }
    }
    
    /**
     * 设置作业暂停状态（使用整合缓存）
     */
    public boolean setJobPausedFromCache(String jobKey, boolean paused) {
        JobData jobData = jobDefCache.get(jobKey);
        if (jobData == null) {
            logger.warn("作业 {} 数据未加载，无法设置暂停状态", jobKey);
            return false;
        }
        
        if (paused) {
            return jobData.updateStatus(ExecutionStatus.PAUSED);
        } else {
            return jobData.updateStatus(ExecutionStatus.RUNNING);
        }
    }
    
    /**
     * 设置作业停止状态（使用整合缓存）
     */
    public boolean setJobStoppedFromCache(String jobKey, boolean stopped) {
        JobData jobData = jobDefCache.get(jobKey);
        if (jobData == null) {
            logger.warn("作业 {} 数据未加载，无法设置停止状态", jobKey);
            return false;
        }
        
        if (stopped) {
            return jobData.updateStatus(ExecutionStatus.STOPPED);
        } else {
            return jobData.updateStatus(ExecutionStatus.RUNNING);
        }
    }
    
    /**
     * 设置作业取消状态（使用整合缓存）
     */
    public boolean setJobCancelledFromCache(String jobKey, boolean cancelled) {
        JobData jobData = jobDefCache.get(jobKey);
        if (jobData == null) {
            logger.warn("作业 {} 数据未加载，无法设置取消状态", jobKey);
            return false;
        }
        
        if (cancelled) {
            return jobData.updateStatus(ExecutionStatus.CANCELLED);
        } else {
            return jobData.updateStatus(ExecutionStatus.STOPPED);
        }
    }

    // ==================== 新增支持批次号的方法实现 ====================
    
    @Override
    public boolean pauseJob(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                jobExecutionLogService.updateStatus(jobData.getLogId(),"PAUSED","暂停任务");  // 使用大写状态值
                boolean result = jobData.updateStatus(ExecutionStatus.PAUSED);
                if (result) {
                    logger.info("作业暂停成功: {} -> PAUSED", jobKey);
                }
                return result;
            } else {
                logger.warn("作业 {} 数据未加载，无法暂停", jobKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("暂停作业失败: {} (批次: {})", jobCode, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean resumeJob(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                jobExecutionLogService.updateStatus(jobData.getLogId(),"RUNNING","恢复任务");  // 使用大写状态值
                boolean result = jobData.updateStatus(ExecutionStatus.RUNNING);
                if (result) {
                    logger.info("作业恢复成功: {} -> RUNNING", jobKey);
                }
                return result;
            } else {
                logger.warn("作业 {} 数据未加载，无法恢复", jobKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("恢复作业失败: {} (批次: {})", jobCode, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean stopJob(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                jobExecutionLogService.updateStatus(jobData.getLogId(),"STOPPED","停止任务");  // 使用大写状态值
                boolean result = jobData.updateStatus(ExecutionStatus.STOPPED);
                if (result) {
                    logger.info("作业停止成功: {} -> STOPPED", jobKey);
                }
                return result;
            } else {
                logger.warn("作业 {} 数据未加载，无法停止", jobKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("停止作业失败: {} (批次: {})", jobCode, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean cancelJob(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                jobExecutionLogService.updateStatus(jobData.getLogId(),"CANCELLED","取消任务");  // 使用大写状态值
                boolean result = jobData.updateStatus(ExecutionStatus.CANCELLED);
                if (result) {
                    logger.info("作业取消成功: {} -> CANCELLED", jobKey);
                }
                return result;
            } else {
                logger.warn("作业 {} 数据未加载，无法取消", jobKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("取消作业失败: {} (批次: {})", jobCode, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean resetJobStatus(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                boolean result = jobData.updateStatus(ExecutionStatus.PENDING);
                if (result) {
                    logger.info("作业状态重置成功: {} -> PENDING", jobKey);
                }
                return result;
            } else {
                logger.warn("作业 {} 数据未加载，无法重置状态", jobKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("重置作业状态失败: {} (批次: {})", jobCode, batchNo, e);
            return false;
        }
    }
    
    @Override
    public void setJobLogId(String jobCode, String batchNo, Long logId) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                jobData.setLogId(logId);
                logger.info("设置作业日志ID成功: {} -> {}", jobKey, logId);
            } else {
                logger.warn("作业 {} 数据未加载，无法设置日志ID", jobKey);
            }
        } catch (Exception e) {
            logger.error("设置作业日志ID失败: {} (批次: {})", jobCode, batchNo, e);
        }
    }
    
    @Override
    public long getJobLogId(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                return jobData.getLogId();
            } else {
                logger.warn("作业 {} 数据未加载，无法获取日志ID", jobKey);
                return -1L;
            }
        } catch (Exception e) {
            logger.error("获取作业日志ID失败: {} (批次: {})", jobCode, batchNo, e);
            return -1L;
        }
    }
    
    @Override
    public void waitForJobPause(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null && ExecutionStatus.PAUSED.equals(jobData.getStatus())) {
                logger.info("作业 {} 已暂停，等待恢复", jobKey);

                while (isJobPaused(jobCode,batchNo)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
                }


            }
        } catch (Exception e) {
            logger.error("等待作业暂停失败: {} (批次: {})", jobCode, batchNo, e);
        }
    }
    
    @Override
    public boolean pauseJobGroup(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                boolean result = groupData.updateStatus(ExecutionStatus.PAUSED);
                if (result) {
                    logger.info("作业组暂停成功: {} -> PAUSED", groupKey);
                }
                return result;
            } else {
                logger.warn("作业组 {} 数据未加载，无法暂停", groupKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("暂停作业组失败: {} (批次: {})", groupName, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean resumeJobGroup(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                boolean result = groupData.updateStatus(ExecutionStatus.RUNNING);
                if (result) {
                    logger.info("作业组恢复成功: {} -> RUNNING", groupKey);
                }
                return result;
            } else {
                logger.warn("作业组 {} 数据未加载，无法恢复", groupKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("恢复作业组失败: {} (批次: {})", groupName, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean stopJobGroup(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                boolean result = groupData.updateStatus(ExecutionStatus.STOPPED);
                if (result) {
                    logger.info("作业组停止成功: {} -> STOPPED", groupKey);
                }
                return result;
            } else {
                logger.warn("作业组 {} 数据未加载，无法停止", groupKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("停止作业组失败: {} (批次: {})", groupName, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean restartJobGroup(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                boolean result = groupData.updateStatus(ExecutionStatus.PENDING);
                if (result) {
                    logger.info("作业组重启成功: {} -> PENDING", groupKey);
                }
                return result;
            } else {
                logger.warn("作业组 {} 数据未加载，无法重启", groupKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("重启作业组失败: {} (批次: {})", groupName, batchNo, e);
            return false;
        }
    }
    
    @Override
    public boolean cancelJobGroup(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                boolean result = groupData.updateStatus(ExecutionStatus.CANCELLED);
                if (result) {
                    logger.info("作业组取消成功: {} -> CANCELLED", groupKey);
                }
                return result;
            } else {
                logger.warn("作业组 {} 数据未加载，无法取消", groupKey);
                return false;
            }
        } catch (Exception e) {
            logger.error("取消作业组失败: {} (批次: {})", groupName, batchNo, e);
            return false;
        }
    }
    
    @Override
    public void updateGroupStatus(String groupName, String batchNo, String status) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                ExecutionStatus executionStatus = ExecutionStatus.valueOf(status.toUpperCase());
                boolean result = groupData.updateStatus(executionStatus);
                if (result) {
                    logger.info("更新作业组状态成功: {} -> {}", groupKey, status);
                } else {
                    logger.warn("更新作业组状态失败: {} -> {}", groupKey, status);
                }
            } else {
                logger.warn("作业组 {} 数据未加载，无法更新状态", groupKey);
            }
        } catch (Exception e) {
            logger.error("更新作业组状态失败: {} (批次: {}) -> {}", groupName, batchNo, status, e);
        }
    }
    
    @Override
    public ExecutionStatus getGroupStatus(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            JobGroupData groupData = jobGroupDataCache.get(groupKey);
            
            if (groupData != null) {
                return groupData.getStatus();
            } else {
                logger.warn("作业组 {} 数据未加载，无法获取状态", groupKey);
                return null;
            }
        } catch (Exception e) {
            logger.error("获取作业组状态失败: {} (批次: {})", groupName, batchNo, e);
            return null;
        }
    }
    
    @Override
    public ExecutionStatus getJobStatus(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobData jobData = jobDefCache.get(jobKey);
            
            if (jobData != null) {
                return jobData.getStatus();
            } else {
                logger.warn("作业 {} 数据未加载，无法获取状态", jobKey);
                return null;
            }
        } catch (Exception e) {
            logger.error("获取作业状态失败: {} (批次: {})", jobCode, batchNo, e);
            return null;
        }
    }
    
    @Override
    public boolean isGroupPaused(String groupName, String batchNo) {
        ExecutionStatus status = getGroupStatus(groupName, batchNo);
        return ExecutionStatus.PAUSED.equals(status);
    }
    
    @Override
    public boolean isGroupStopped(String groupName, String batchNo) {
        ExecutionStatus status = getGroupStatus(groupName, batchNo);
        return ExecutionStatus.STOPPED.equals(status);
    }
    
    @Override
    public boolean isGroupCancelled(String groupName, String batchNo) {
        ExecutionStatus status = getGroupStatus(groupName, batchNo);
        return ExecutionStatus.CANCELLED.equals(status);
    }
    
    @Override
    public boolean isJobPaused(String jobCode, String batchNo) {
        ExecutionStatus status = getJobStatus(jobCode, batchNo);
        return ExecutionStatus.PAUSED.equals(status);
    }
    
    @Override
    public boolean isJobStopped(String jobCode, String batchNo) {
        ExecutionStatus status = getJobStatus(jobCode, batchNo);
        return ExecutionStatus.STOPPED.equals(status);
    }
    
    @Override
    public boolean isJobCancelled(String jobCode, String batchNo) {
        ExecutionStatus status = getJobStatus(jobCode, batchNo);
        return ExecutionStatus.CANCELLED.equals(status);
    }
    
    @Override
    public void loadJobGroupData(String groupName, String batchNo) {
        try {
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            
            if (jobGroupDataCache.containsKey(groupKey)) {
                logger.debug("作业组 {} 数据已缓存，跳过加载", groupKey);
                return;
            }
            
            initializeJobGroupData(groupName, batchNo);
            
        } catch (Exception e) {
            logger.error("加载作业组数据失败: {} (批次: {})", groupName, batchNo, e);
        }
    }
    
    @Override
    public void cleanupBatchData(String batchNo) {
        try {
            logger.info("开始清理批次数据: {}", batchNo);
            
            // 清理作业组缓存
            jobGroupDataCache.entrySet().removeIf(entry -> {
                String key = entry.getKey();
                return CacheKeyUtils.isKeyBelongsToBatch(key, batchNo);
            });
            
            // 清理作业缓存
            jobDefCache.entrySet().removeIf(entry -> {
                String key = entry.getKey();
                return CacheKeyUtils.isKeyBelongsToBatch(key, batchNo);
            });
            
            // 清理活跃作业组
            activeJobGroups.removeIf(key -> CacheKeyUtils.isKeyBelongsToBatch(key, batchNo));
            
            // 清理锁
            groupLocks.entrySet().removeIf(entry -> 
                CacheKeyUtils.isKeyBelongsToBatch(entry.getKey(), batchNo));
            jobLocks.entrySet().removeIf(entry -> 
                CacheKeyUtils.isKeyBelongsToBatch(entry.getKey(), batchNo));
            
            // 清理统计
            groupStatistics.entrySet().removeIf(entry -> 
                CacheKeyUtils.isKeyBelongsToBatch(entry.getKey(), batchNo));
            
            logger.info("批次数据清理完成: {}", batchNo);
            
        } catch (Exception e) {
            logger.error("清理批次数据失败: {}", batchNo, e);
        }
    }
    
    @Override
    public List<String> getJobCodesByBatch(String batchNo) {
        try {
            List<String> jobCodes = new ArrayList<>();
            
            for (String key : jobDefCache.keySet()) {
                if (CacheKeyUtils.isKeyBelongsToBatch(key, batchNo)) {
                    String jobCode = CacheKeyUtils.extractJobCode(key);
                    if (StringUtils.hasText(jobCode)) {
                        jobCodes.add(jobCode);
                    }
                }
            }
            
            logger.debug("获取批次作业代码: {} -> {}", batchNo, jobCodes.size());
            return jobCodes;
            
        } catch (Exception e) {
            logger.error("获取批次作业代码失败: {}", batchNo, e);
            return Collections.emptyList();
        }
    }
    
    @Override
    public List<String> getGroupNamesByBatch(String batchNo) {
        try {
            List<String> groupNames = new ArrayList<>();
            
            for (String key : jobGroupDataCache.keySet()) {
                if (CacheKeyUtils.isKeyBelongsToBatch(key, batchNo)) {
                    String groupName = CacheKeyUtils.extractGroupName(key);
                    if (StringUtils.hasText(groupName)) {
                        groupNames.add(groupName);
                    }
                }
            }
            
            logger.debug("获取批次作业组名称: {} -> {}", batchNo, groupNames.size());
            return groupNames;
            
        } catch (Exception e) {
            logger.error("获取批次作业组名称失败: {}", batchNo, e);
            return Collections.emptyList();
        }
    }
    
    @Override
    public com.aia.gdp.dto.BatchStatistics getBatchStatisticsFromDatabase(String batchNo) {
        try {
            logger.debug("从数据库获取批次 {} 统计信息", batchNo);
            
            // 查询执行日志表
            List<JobExecutionLog> logs = jobExecutionLogMapper.selectByBatchNo(batchNo);
            
            if (logs.isEmpty()) {
                logger.warn("批次 {} 在数据库中未找到执行日志", batchNo);
                return null;
            }
            
            // 创建统计对象
            com.aia.gdp.dto.BatchStatistics statistics = new com.aia.gdp.dto.BatchStatistics(batchNo);
            statistics.setDataSource("DATABASE");
            
            // 统计各种状态
            int totalJobs = logs.size();
            int runningJobs = 0;
            int completedJobs = 0;
            int failedJobs = 0;
            int pausedJobs = 0;
            int stoppedJobs = 0;
            int cancelledJobs = 0;
            
            for (JobExecutionLog log : logs) {
                String status = log.getStatus();
                if (status != null) {
                    switch (status.toUpperCase()) {
                        case "RUNNING":
                            runningJobs++;
                            break;
                        case "SUCCESS":
                            completedJobs++;
                            break;
                        case "FAILED":
                            failedJobs++;
                            break;
                        case "PAUSED":
                            pausedJobs++;
                            break;
                        case "STOPPED":
                            stoppedJobs++;
                            break;
                        case "CANCELLED":
                            cancelledJobs++;
                            break;
                        default:
                            // 其他状态，可以记录日志
                            logger.debug("未知状态: {} for job: {}", status, log.getJobCode());
                            break;
                    }
                }
            }
            
            // 设置统计结果
            statistics.setTotalJobs(totalJobs);
            statistics.setRunningJobs(runningJobs);
            statistics.setCompletedJobs(completedJobs);
            statistics.setFailedJobs(failedJobs);
            statistics.setPausedJobs(pausedJobs);
            statistics.setStoppedJobs(stoppedJobs);
            statistics.setCancelledJobs(cancelledJobs);
            
            // 计算成功率
            if (totalJobs > 0) {
                double successRate = (double) completedJobs / totalJobs * 100;
                statistics.setSuccessRate(Math.round(successRate * 100.0) / 100.0);
        } else {
                statistics.setSuccessRate(0.0);
            }
            
            logger.info("批次 {} 统计完成: 总数={}, 运行中={}, 已完成={}, 失败={}, 暂停={}, 停止={}, 取消={}, 成功率={}%", 
                batchNo, totalJobs, runningJobs, completedJobs, failedJobs, pausedJobs, stoppedJobs, cancelledJobs, statistics.getSuccessRate());
            
            return statistics;
            
        } catch (Exception e) {
            logger.error("从数据库获取批次统计信息失败: {}", batchNo, e);
            return null;
        }
    }
    
    // ==================== 根据执行日志ID操作 ====================
    
    /**
     * 根据执行日志ID暂停作业
     */
    @Override
    public boolean pauseJobByLogId(Long logId) {
        try {
            if (logId == null) {
                logger.warn("执行日志ID为空，无法暂停作业");
                return false;
            }
            
            // 从JobExecutionLog中获取jobCode和batchNo
            JobExecutionLog jobLog = getJobByLogId(logId);
            if (jobLog == null) {
                logger.warn("根据执行日志ID {} 未找到作业定义", logId);
                return false;
            }
            

            return pauseJob(jobLog.getJobCode(), jobLog.getBatchNo());
            
        } catch (Exception e) {
            logger.error("根据执行日志ID {} 暂停作业失败", logId, e);
            return false;
        }
    }
    
    /**
     * 根据执行日志ID恢复作业
     */
    @Override
    public boolean resumeJobByLogId(Long logId) {
        try {
            if (logId == null) {
                logger.warn("执行日志ID为空，无法恢复作业");
                return false;
            }
            
            JobExecutionLog jobLog = getJobByLogId(logId);
            if (jobLog == null) {
                logger.warn("根据执行日志ID {} 未找到作业定义", logId);
                return false;
            }

            return resumeJob(jobLog.getJobCode(), jobLog.getBatchNo());
            
        } catch (Exception e) {
            logger.error("根据执行日志ID {} 恢复作业失败", logId, e);
            return false;
        }
    }
    
    /**
     * 根据执行日志ID停止作业
     */
    @Override
    public boolean stopJobByLogId(Long logId) {
        try {
            if (logId == null) {
                logger.warn("执行日志ID为空，无法停止作业");
                return false;
            }
            
            JobExecutionLog jobLog = getJobByLogId(logId);
            if (jobLog == null) {
                logger.warn("根据执行日志ID {} 未找到作业定义", logId);
                return false;
            }
                
            String batchNo = ""; // TODO: 从JobExecutionLog中获取batchNo
            
            return stopJob(jobLog.getJobCode(),jobLog.getBatchNo());
            
        } catch (Exception e) {
            logger.error("根据执行日志ID {} 停止作业失败", logId, e);
            return false;
        }
    }

    /**
     * 根据执行日志ID取消作业
     */
    @Override
    public boolean cancelJobByLogId(Long logId) {
        try {
            if (logId == null) {
                logger.warn("执行日志ID为空，无法取消作业");
            return false;
        }

            JobExecutionLog jobLog = getJobByLogId(logId);
            if (jobLog == null) {
                logger.warn("根据执行日志ID {} 未找到作业定义", logId);
                return false;
            }

            return cancelJob(jobLog.getJobCode(), jobLog.getBatchNo());
            
        } catch (Exception e) {
            logger.error("根据执行日志ID {} 取消作业失败", logId, e);
            return false;
        }
    }
    
    /**
     * 根据执行日志ID获取作业信息
     */
    @Override
    public JobExecutionLog getJobByLogId(Long logId) {
        try {
            if (logId == null) {
                logger.warn("执行日志ID为空，无法获取作业信息");
                return null;
            }
            JobExecutionLog jobLog = jobExecutionLogService.getById(logId);
            if (jobLog == null) {
                logger.warn("根据执行日志ID {} 未找到作业", logId);
                return null;
            }

            return jobLog;
            
        } catch (Exception e) {
            logger.error("根据执行日志ID {} 获取作业信息失败", logId, e);
            return null;
        }
    }
} 