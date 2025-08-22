package com.aia.gdp.handler;

import com.aia.gdp.dto.JobExecutionResult;
import com.aia.gdp.event.JobControlEvent;
import com.aia.gdp.model.JobDef;
import com.aia.gdp.model.JobExecutionLog;
import com.aia.gdp.service.EmailNotificationService;
import com.aia.gdp.service.JobControlService;
import com.aia.gdp.service.JobDefService;
import com.aia.gdp.service.JobExecutionLogService;
import com.aia.gdp.service.JobControlService.ExecutionStatus;
import com.aia.gdp.common.Utils;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.context.XxlJobHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import com.aia.gdp.common.CacheKeyUtils;
import java.util.*;
import java.util.concurrent.*;
/**
 * XXL-JOB 执行器 - 作业分组调度处理器
 * 
 * 功能特性：
 * - 支持多分组作业执行
 * - 组内同步执行，组间可配置同步/异步
 * - 智能状态管理和健康检查
 * - 完整的执行监控和通知机制
 *
 * @author andy
 * @date 2025-07-29
 * @version 2.0
 */
@Component
public class JobGroupDispatcherHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(JobGroupDispatcherHandler.class);
    
    // ==================== 依赖注入 ====================
    @Autowired
    private JobDefService jobDefService;
    
    @Autowired
    private WorkerHandler workerHandler;
    
    @Autowired
    private JobExecutionLogService jobExecutionLogService;
    
    @Autowired
    private EmailNotificationService emailNotificationService;
    
    @Autowired
    private JobControlService jobControlService;
    
    @Autowired
    private ApplicationEventPublisher eventPublisher;
    
    // ==================== 配置参数 ====================
    @Value("${jobs.group.execution-mode:sync}")
    private String groupExecutionMode;

    @Value("${jobs.group.async.thread-pool-size:10}")
    private int asyncThreadPoolSize;
    
    @Value("${jobs.group.timeout.seconds:3600}")
    private int groupTimeoutSeconds;
    
    @Value("${jobs.group.notification.enabled:true}")
    private boolean notificationEnabled;

    /**
     * 作业分组调度器主入口
     * 
     * 执行流程：
     * 1. 解析作业分组参数
     * 2. 生成本次执行批次号
     * 3. 执行分组作业（同步/异步）
     * 4. 触发完成通知
     * 5. 记录执行统计
     * 
     * @return void
     */
    @XxlJob("jobGroupDispatcher")
    public void execute() {
        long startTime = System.currentTimeMillis();
        String jobGroupParam = XxlJobHelper.getJobParam();
        
        try {
            // 1. 解析作业分组
            Set<String> jobGroups = parseJobGroups(jobGroupParam);
            if (jobGroups.isEmpty()) {
                XxlJobHelper.log("未找到任何作业分组");
                return;
            }
            
            // 2. 生成本次执行的批次号
            String batchNo = Utils.generateBatchNo();
            XxlJobHelper.log("生成本次执行批次号: {}", batchNo);
            
            XxlJobHelper.log("开始执行作业分组调度，分组数量: {}, 执行模式: {}, 批次号: {}", 
                           jobGroups.size(), groupExecutionMode, batchNo);
            
            // 3. 执行分组作业
            GroupExecutionResult result = executeJobGroups(jobGroups, batchNo);
            
            // 4. 触发完成通知
            if (notificationEnabled) {
                triggerCompletionNotification(result);
            }
            
            // 5. 记录执行统计
            long durationMs = System.currentTimeMillis() - startTime;
            int durationSeconds = (int) (durationMs / 1000);
            logExecutionSummary(result, durationSeconds);
            
        } catch (Exception e) {
            logger.error("作业分组调度执行异常", e);
            XxlJobHelper.log("作业分组调度执行异常: " + e.getMessage());
            throw e;
        }
    }


    /**
     * 解析作业分组参数
     * 
     * 逻辑说明：
     * - 如果未指定分组，则获取所有活跃分组
     * - 如果指定了分组，则解析逗号分隔的分组列表
     * - 使用 LinkedHashSet 保持分组顺序
     * 
     * @param jobGroupParam 作业分组参数，格式：GROUP_A,GROUP_B 或 null
     * @return 作业分组集合
     */
    private Set<String> parseJobGroups(String jobGroupParam) {
        Set<String> jobGroups = new LinkedHashSet<>();
        
        if (jobGroupParam == null || jobGroupParam.trim().isEmpty()) {
            // 获取所有活跃分组
            jobGroups = jobControlService.getActiveJobGroups();
            logger.debug("未指定分组，获取所有活跃分组: {}", jobGroups);
                    } else {
            // 解析指定分组列表
            String[] groupArray = jobGroupParam.split(",");
            for (String group : groupArray) {
                String trimmedGroup = group.trim();
                if (!trimmedGroup.isEmpty()) {
                        jobGroups.add(trimmedGroup);
                    logger.debug("添加指定的作业组: {}", trimmedGroup);
                }
            }
        }
        
        logger.info("解析作业组完成，共 {} 个组: {}", jobGroups.size(), jobGroups);
        return jobGroups;
    }

    /**
     * 执行作业分组
     * 
     * 执行流程：
     * 1. 初始化所有作业组状态
     * 2. 根据配置选择同步或异步执行模式
     * 3. 返回执行结果
     * 
     * @param jobGroups 作业分组集合
     * @param batchNo 批次号
     * @return 分组执行结果
     */
    private GroupExecutionResult executeJobGroups(Set<String> jobGroups, String batchNo) {
        GroupExecutionResult result = new GroupExecutionResult();
        result.setTotalGroups(jobGroups.size());
        
        try {
            // 🎯 统一初始化所有作业组状态
            logger.info("开始初始化所有作业组，共 {} 个组", jobGroups.size());
            initializeAllJobGroups(jobGroups, batchNo);
            
            logger.info("所有作业组初始化完成，开始执行");
            
            // 根据配置选择执行模式
        if ("async".equalsIgnoreCase(groupExecutionMode)) {
            return executeGroupsAsync(jobGroups, result, batchNo);
        } else {
            return executeGroupsSync(jobGroups, result, batchNo);
            }
            
        } catch (Exception e) {
            logger.error("执行作业分组时发生异常", e);
            throw e;
        }
    }

    /**
     * 初始化所有作业组状态
     * 
     * @param jobGroups 作业分组集合
     * @param batchNo 批次号
     */
    private void initializeAllJobGroups(Set<String> jobGroups, String batchNo) {
        for (String groupName : jobGroups) {
            try {
                // 1. 加载作业组数据
                jobControlService.loadJobGroupData(groupName,batchNo);
                
                // 2. 设置作业组状态为 RUNNING
                jobControlService.updateGroupStatus(groupName,batchNo, "RUNNING");
                
                // 3. 初始化作业组内所有作业
                initializeGroupJobs(groupName, batchNo);
                
                logger.info("作业组 {} 初始化完成", groupName);
            } catch (Exception e) {
                logger.error("作业组 {} 初始化失败", groupName, e);
            }
        }
    }
    
    /**
     * 初始化作业组内所有作业
     * 
     * @param groupName 作业组名称
     * @param batchNo 批次号
     */
    private void initializeGroupJobs(String groupName, String batchNo) {
        List<JobDef> groupJobs = jobControlService.getJobGroupData(groupName,batchNo);
        if (groupJobs == null || groupJobs.isEmpty()) {
            logger.warn("作业组 {} 内无有效作业", groupName);
            return;
        }
        
        // 🎯 使用安全的遍历方式，避免 ConcurrentModificationException
        List<JobDef> jobsCopy = new ArrayList<>(groupJobs);
        for (JobDef job : jobsCopy) {
            if (job.getIsActive()) {
                try {
                    // 重置作业状态为初始状态
                    jobControlService.resetJobStatus(job.getJobCode(),batchNo);
                    
                    // 创建执行日志记录
                    JobExecutionLog executionLog = createInitialExecutionLog(job, batchNo);
                    jobExecutionLogService.save(executionLog);
                    
                    // 设置作业日志ID
                    jobControlService.setJobLogId(job.getJobCode(),batchNo, executionLog.getLogId());
                    
                    logger.debug("作业 {} 初始化完成，logId={}", job.getJobCode(), executionLog.getLogId());
            } catch (Exception e) {
                    logger.error("作业 {} 初始化失败", job.getJobCode(), e);
                }
            }
        }
    }
    
    /**
     * 创建初始执行日志记录
     * 
     * @param job 作业定义
     * @param batchNo 批次号
     * @return 执行日志记录
     */
    private JobExecutionLog createInitialExecutionLog(JobDef job, String batchNo) {
        JobExecutionLog executionLog = new JobExecutionLog();
        executionLog.setJobCode(job.getJobCode());
        executionLog.setBatchNo(batchNo);
        executionLog.setStatus("pending");
        executionLog.setStartTime(new Date());
        executionLog.setExecutorProc(job.getJobType());
        executionLog.setExecutorAddress(getExecutorAddress());
        return executionLog;
    }

    /**
     * 同步执行作业分组
     * 
     * 执行特点：
     * - 按顺序逐个执行作业组
     * - 组内作业同步执行
     * - 提供详细的执行进度和状态
     * 
     * @param jobGroups 作业分组集合
     * @param result 执行结果对象
     * @param batchNo 批次号
     * @return 分组执行结果
     */
    private GroupExecutionResult executeGroupsSync(Set<String> jobGroups, GroupExecutionResult result, String batchNo) {
        logger.info("开始同步执行作业分组，共 {} 个组", jobGroups.size());
        
        List<String> groupList = new ArrayList<>(jobGroups);
        int totalGroups = groupList.size();
        
        for (int i = 0; i < groupList.size(); i++) {
            String groupName = groupList.get(i);
            
            try {
                logger.info("开始执行作业组 {}/{}: {}", (i + 1), totalGroups, groupName);
                
                // 执行单个分组
                GroupJobResult groupResult = executeSingleGroup(groupName, batchNo);
                
                // 更新总体结果
                result.addGroupResult(groupResult);
                
                logger.info("作业组 {}/{} 执行完成: {}", (i + 1), totalGroups, groupName);
                
            } catch (Exception e) {
                logger.error("执行作业组 {} 时发生异常", groupName, e);
                GroupJobResult errorResult = new GroupJobResult();
                errorResult.setGroupName(groupName);
                errorResult.setErrorMessage("执行异常: " + e.getMessage());
                result.addGroupResult(errorResult);
                result.incrementFailureGroups();
            }
            // 注意：作业组清理在 JobControlEventListener 中处理，避免内存泄漏
        }
        
        logger.info("同步执行完成，总计: {}, 成功: {}, 失败: {}, 跳过: {}", 
                   totalGroups, result.getSuccessGroups(), result.getFailureGroups(), result.getSkippedGroups());
        
        return result;
    }

    /**
     * 异步执行作业分组
     * 
     * 执行特点：
     * - 使用线程池并发执行多个作业组
     * - 支持超时控制和异常处理
     * - 确保线程池资源正确释放
     * 
     * @param jobGroups 作业分组集合
     * @param result 执行结果对象
     * @param batchNo 批次号
     * @return 分组执行结果
     */
    private GroupExecutionResult executeGroupsAsync(Set<String> jobGroups, GroupExecutionResult result, String batchNo) {
        XxlJobHelper.log("开始异步执行 {} 个分组，线程池大小: {}", jobGroups.size(), asyncThreadPoolSize);
        
        ExecutorService executor = null;
        Map<String, Future<GroupJobResult>> futures = new HashMap<>();
        
        try {
            // 创建固定大小的线程池
            executor = Executors.newFixedThreadPool(asyncThreadPoolSize);
            
            // 提交所有分组任务
            for (String group : jobGroups) {
                Future<GroupJobResult> future = executor.submit(() -> executeSingleGroup(group, batchNo));
                futures.put(group, future);
            }
            
            // 等待所有任务完成并收集结果
            collectAsyncResults(futures, result);
            
        } catch (Exception e) {
            logger.error("异步执行分组时发生异常", e);
        } finally {
            // 确保线程池在所有情况下都能正确关闭
            shutdownExecutorSafely(executor);
        }
        
        return result;
    }
    
    /**
     * 收集异步执行结果
     * 
     * @param futures 异步任务映射
     * @param result 执行结果对象
     */
    private void collectAsyncResults(Map<String, Future<GroupJobResult>> futures, GroupExecutionResult result) {
            for (Map.Entry<String, Future<GroupJobResult>> entry : futures.entrySet()) {
                String group = entry.getKey();
                Future<GroupJobResult> future = entry.getValue();
                
                try {
                    GroupJobResult groupResult = future.get(groupTimeoutSeconds, TimeUnit.SECONDS);
                    result.addGroupResult(group, groupResult);
                    
                    XxlJobHelper.log("分组 {} 异步执行完成 - 成功: {}, 失败: {}", 
                                   group, groupResult.getSuccessCount(), groupResult.getFailureCount());
                    
                } catch (TimeoutException e) {
                    logger.error("分组 {} 执行超时", group);
                handleAsyncExecutionError(group, "执行超时", result);
                } catch (Exception e) {
                    logger.error("分组 {} 异步执行异常", group, e);
                handleAsyncExecutionError(group, e.getMessage(), result);
            }
        }
    }
    
    /**
     * 处理异步执行错误
     * 
     * @param group 作业组名称
     * @param errorMessage 错误信息
     * @param result 执行结果对象
     */
    private void handleAsyncExecutionError(String group, String errorMessage, GroupExecutionResult result) {
                    GroupJobResult errorResult = new GroupJobResult();
        errorResult.setGroupName(group);
        errorResult.setErrorMessage(errorMessage);
                    result.addGroupResult(group, errorResult);
                }
    
    /**
     * 安全关闭线程池
     * 
     * @param executor 线程池
     */
    private void shutdownExecutorSafely(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        logger.warn("线程池关闭超时，强制关闭");
                        executor.shutdownNow();
                
                        // 再次等待强制关闭
                        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                            logger.error("线程池强制关闭失败");
                        }
                    }
                } catch (InterruptedException e) {
                    logger.warn("等待线程池关闭时被中断");
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
            }
    }

    /**
     * 执行单个分组内的所有作业
     * 
     * 执行流程：
     * 1. 获取分组内所有活跃作业
     * 2. 逐个执行作业，支持暂停/恢复控制
     * 3. 记录执行结果和统计信息
     * 4. 处理成功/失败通知
     * 
     * @param jobGroup 作业组名称
     * @param batchNo 批次号
     * @return 分组作业执行结果
     */
    private GroupJobResult executeSingleGroup(String jobGroup, String batchNo) {
        GroupJobResult result = new GroupJobResult();
        result.setGroupName(jobGroup);
        
        try {
            // 获取分组内按顺序排列的作业
            List<JobDef> jobs = jobControlService.getJobGroupData(jobGroup,batchNo);
            if (jobs == null || jobs.isEmpty()) {
                result.setErrorMessage("分组内无有效作业");
                logger.warn("分组 {} 内无有效作业", jobGroup);
                jobControlService.completeGroup(jobGroup,batchNo,false);
                return result;
            }
            
            // 🎯 创建作业列表的副本，避免遍历时的并发修改问题
            List<JobDef> jobsCopy = new ArrayList<>(jobs);
            
            // 设置总作业数
            result.setTotalJobs(jobsCopy.size());
            logger.info("开始执行作业组: {}, 共 {} 个作业", jobGroup, jobsCopy.size());
            
            // 记录开始时间
            long startTime = System.currentTimeMillis();
            
            // 逐个执行作业
            executeGroupJobs(jobsCopy, jobGroup, batchNo, result, startTime);
            
        } catch (Exception e) {
            logger.error("执行作业组 {} 时发生异常", jobGroup, e);
            result.setErrorMessage("执行异常: " + e.getMessage());
            jobControlService.completeGroup(jobGroup, batchNo,false);
        }
        
        return result;
    }
    
    /**
     * 执行分组内的所有作业
     * 
     * @param jobs 作业列表
     * @param jobGroup 作业组名称
     * @param batchNo 批次号
     * @param result 执行结果对象
     * @param startTime 开始时间
     */
    private void executeGroupJobs(List<JobDef> jobs, String jobGroup, String batchNo, 
                                 GroupJobResult result, long startTime) {
        boolean eventPublished = false;
        
        for (int i = 0; i < jobs.size(); i++) {
            JobDef job = jobs.get(i);
            
            try {
                if (!job.getIsActive()) {
                    logger.debug("跳过非活跃作业: {}", job.getJobCode());
                        continue;
                    }
                    
                    // 执行单个作业
                executeSingleJob(job, jobGroup, batchNo, result, startTime, eventPublished);
                eventPublished = true;
                
            } catch (Exception e) {
                result.incrementFailureCount();
                logger.error("作业 {} 执行异常", job.getJobCode(), e);
            }
        }
    }
    
    /**
     * 执行单个作业
     * 
     * @param job 作业定义
     * @param jobGroup 作业组名称
     * @param batchNo 批次号
     * @param result 执行结果对象
     * @param startTime 开始时间
     * @param eventPublished 是否已发布事件
     */
    private void executeSingleJob(JobDef job, String jobGroup, String batchNo, 
                                 GroupJobResult result, long startTime, boolean eventPublished) {
        try {
            //String jobKey = CacheKeyUtils.generateJobKey(job.getJobCode(), batchNo);
            //String groupKey = CacheKeyUtils.generateGroupKey(jobGroup, batchNo);
            // 🎯 为每个作业发布执行注册事件
                        eventPublisher.publishEvent(new JobControlEvent.JobExecutionRegisteredEvent(
                            job.getJobCode(), batchNo, jobGroup));
            
            long logId = jobControlService.getJobLogId(job.getJobCode(),batchNo);

            // 等待作业组和作业的暂停/恢复控制
            jobControlService.waitForGroupPause(job.getJobGroup(),batchNo);
            jobControlService.waitForJobPause(job.getJobCode(),batchNo);
            // 检查作业当前状态，如果是暂停状态则先恢复
            ExecutionStatus currentStatus = jobControlService.getJobStatus(job.getJobCode(), batchNo);
            if (currentStatus == ExecutionStatus.PAUSED) {
                logger.info("作业 {} 当前处于暂停状态，先恢复再执行", job.getJobCode());
                // 恢复作业状态
                jobControlService.resumeJob(job.getJobCode(), batchNo);
                // 更新执行日志状态为RUNNING
                jobExecutionLogService.updateStatus(logId, "RUNNING", "恢复暂停的作业并开始执行");
            } else {
                // 更新执行日志状态为 RUNNING
                jobExecutionLogService.updateStatus(logId, "RUNNING", "开始执行作业");
            }
            

                    
            // 实际执行作业
            JobExecutionResult jobResult = workerHandler.executeJob(job, batchNo);

            // 计算总耗时（毫秒转秒）
            long totalDurationMs = System.currentTimeMillis() - startTime;
            int totalDurationSeconds = (int) (totalDurationMs / 1000);
            result.setDuration(totalDurationSeconds);
                    
                    // 更新执行日志
            updateJobExecutionLog(logId, jobResult);
            
            // 发布作业执行完成事件
            eventPublisher.publishEvent(new JobControlEvent.JobExecutionCompletedEvent(jobGroup,batchNo));
            
            // 处理执行结果
            handleJobExecutionResult(job, batchNo,jobResult, result, logId);
                    
                } catch (Exception e) {
            logger.error("作业 {} 执行过程中发生异常", job.getJobCode(), e);
            throw e;
        }
    }
    
    /**
     * 更新作业执行日志
     * 
     * @param logId 日志ID
     * @param jobResult 作业执行结果
     */
    private void updateJobExecutionLog(long logId, JobExecutionResult jobResult) {
        // 根据执行结果更新状态
        String finalStatus = jobResult.isSuccess() ? "SUCCESS" : "FAILED";  // 使用大写状态值
        jobExecutionLogService.updateStatus(logId, finalStatus,
                jobResult.isSuccess() ? "作业执行成功" : jobResult.getErrorMessage());
        
        // 更新执行时间和耗时
        jobExecutionLogService.updateExecutionTime(logId,
                jobResult.getEndTime(), jobResult.getDuration());
    }
    
    /**
     * 处理作业执行结果
     * 
     * @param job 作业定义
     * @param jobResult 作业执行结果
     * @param result 执行结果对象
     * @param logId 日志ID
     */
    private void handleJobExecutionResult(JobDef job,String batchNo, JobExecutionResult jobResult,
                                        GroupJobResult result, long logId) {
        if (jobResult.isSuccess()) {
            result.incrementSuccessCount();
            jobControlService.completeJob(job.getJobCode(),batchNo,true);
            XxlJobHelper.log("作业 {} 执行成功", job.getJobCode());
            logger.info("作业 {} 执行成功", job.getJobCode());
            } else {
            result.incrementFailureCount();
            result.addFailedJob(job.getJobCode(), jobResult.getErrorMessage());
            jobControlService.completeJob(job.getJobCode(),batchNo, false);
            XxlJobHelper.log("作业 {} 执行失败: {}", job.getJobCode(), jobResult.getErrorMessage());
            logger.warn("作业 {} 执行失败: {}", job.getJobCode(), jobResult.getErrorMessage());
            
            // 发送失败通知
            sendFailureNotification(job, logId);
        }
    }
    
    /**
     * 发送失败通知
     * 
     * @param job 作业定义
     * @param logId 日志ID
     */
    private void sendFailureNotification(JobDef job, long logId) {
        try {
            JobExecutionLog log = jobExecutionLogService.getById(logId);
            emailNotificationService.sendFailureNotification(job, log);
        } catch (Exception e) {
            logger.error("发送失败通知失败: {}", job.getJobCode(), e);
            // 不因为通知失败而中断作业执行
        }
    }

    /**
     * 触发完成通知
     * 
     * 通知流程：
     * 1. 构建通知内容
     * 2. 发送通知
     * 3. 记录通知结果
     * 
     * @param result 执行结果
     */
    private void triggerCompletionNotification(GroupExecutionResult result) {
        try {
            // 构建通知内容
            NotificationContent content = buildNotificationContent(result);
            
            // 发送通知
            sendNotification(content);
            
            XxlJobHelper.log("完成通知已发送");
            
        } catch (Exception e) {
            logger.error("发送完成通知失败", e);
            XxlJobHelper.log("发送完成通知失败: " + e.getMessage());
        }
    }

    /**
     * 构建通知内容
     * 
     * @param result 执行结果
     * @return 通知内容对象
     */
    private NotificationContent buildNotificationContent(GroupExecutionResult result) {
        NotificationContent content = new NotificationContent();
        content.setTotalGroups(result.getTotalGroups());
        content.setSuccessGroups(result.getSuccessGroups());
        content.setFailureGroups(result.getFailureGroups());
        content.setTotalJobs(result.getTotalJobs());
        content.setSuccessJobs(result.getSuccessJobs());
        content.setFailureJobs(result.getFailureJobs());
        content.setExecutionTime(new Date());
        content.setGroupResults(result.getGroupResults());
        
        return content;
    }

    /**
     * 发送通知
     * 
     * 支持的通知方式：
     * - 邮件通知（当前实现）
     * - 短信通知（可扩展）
     * - 钉钉/企业微信通知（可扩展）
     * - 回调接口通知（可扩展）
     * 
     * @param content 通知内容
     */
    private void sendNotification(NotificationContent content) {
        try {
            // 发送邮件通知
        emailNotificationService.sendCompletionNotification(content);
            logger.info("作业分组执行完成通知已发送: {}", content);
        } catch (Exception e) {
            logger.error("发送邮件通知失败", e);
            // 可以在这里添加其他通知方式的降级处理
        }
    }

    /**
     * 记录执行摘要
     * 
     * 记录内容：
     * - 执行结果统计
     * - 执行耗时
     * - 失败作业详情
     * 
     * @param result 执行结果
     * @param duration 执行耗时（秒）
     */
    private void logExecutionSummary(GroupExecutionResult result, int duration) {
        XxlJobHelper.log("=== 作业分组调度执行完成 ===");
        XxlJobHelper.log("总分组数: {}", result.getTotalGroups());
        XxlJobHelper.log("成功分组数: {}", result.getSuccessGroups());
        XxlJobHelper.log("失败分组数: {}", result.getFailureGroups());
        XxlJobHelper.log("总作业数: {}", result.getTotalJobs());
        XxlJobHelper.log("成功作业数: {}", result.getSuccessJobs());
        XxlJobHelper.log("失败作业数: {}", result.getFailureJobs());
        XxlJobHelper.log("总耗时: {} 秒", duration);
        
        if (!result.getFailedJobs().isEmpty()) {
            XxlJobHelper.log("失败作业详情: {}", result.getFailedJobs());
        }
    }

    /**
     * 作业分组执行结果
     * 
     * 包含：
     * - 分组执行统计（总数、成功、失败）
     * - 作业执行统计（总数、成功、失败）
     * - 分组结果映射
     * - 失败作业列表
     */
    public static class GroupExecutionResult {
        
        // ==================== 分组统计 ====================
        private int totalGroups;
        private int successGroups;
        private int failureGroups;
        private int skippedGroups;
        
        // ==================== 作业统计 ====================
        private int totalJobs;
        private int successJobs;
        private int failureJobs;
        
        // ==================== 结果存储 ====================
        private Map<String, GroupJobResult> groupResults = new HashMap<>();
        private List<String> failedJobs = new ArrayList<>();

        /**
         * 添加分组结果（通过对象）
         */
        public void addGroupResult(GroupJobResult result) {
            if (result != null && result.getGroupName() != null) {
                groupResults.put(result.getGroupName(), result);
                
                if (result.getErrorMessage() == null) {
                    successGroups++;
                    totalJobs += result.getTotalJobs();
                    successJobs += result.getSuccessCount();
                    failureJobs += result.getFailureCount();
                } else {
                    failureGroups++;
                }
                
                failedJobs.addAll(result.getFailedJobs());
            }
        }

        /**
         * 添加分组结果（通过名称和对象）
         */
        public void addGroupResult(String groupName, GroupJobResult result) {
            groupResults.put(groupName, result);
            
            if (result.getErrorMessage() == null) {
                successGroups++;
                totalJobs += result.getTotalJobs();
                successJobs += result.getSuccessCount();
                failureJobs += result.getFailureCount();
            } else {
                failureGroups++;
            }
            
            failedJobs.addAll(result.getFailedJobs());
        }

        /**
         * 增加跳过分组计数
         */
        public void incrementSkippedGroups() {
            skippedGroups++;
        }
        
        /**
         * 增加失败分组计数
         */
        public void incrementFailureGroups() {
            failureGroups++;
        }

        // ==================== Getter 和 Setter 方法 ====================
        public int getTotalGroups() { return totalGroups; }
        public void setTotalGroups(int totalGroups) { this.totalGroups = totalGroups; }
        
        public int getSuccessGroups() { return successGroups; }
        public void setSuccessGroups(int successGroups) { this.successGroups = successGroups; }
        
        public int getFailureGroups() { return failureGroups; }
        public void setFailureGroups(int failureGroups) { this.failureGroups = failureGroups; }
        
        public int getSkippedGroups() { return skippedGroups; }
        public void setSkippedGroups(int skippedGroups) { this.skippedGroups = skippedGroups; }
        
        public int getTotalJobs() { return totalJobs; }
        public void setTotalJobs(int totalJobs) { this.totalJobs = totalJobs; }
        
        public int getSuccessJobs() { return successJobs; }
        public void setSuccessJobs(int successJobs) { this.successJobs = successJobs; }
        
        public int getFailureJobs() { return failureJobs; }
        public void setFailureJobs(int failureJobs) { this.failureJobs = failureJobs; }
        
        public Map<String, GroupJobResult> getGroupResults() { return groupResults; }
        public void setGroupResults(Map<String, GroupJobResult> groupResults) { this.groupResults = groupResults; }
        
        public List<String> getFailedJobs() { return failedJobs; }
        public void setFailedJobs(List<String> failedJobs) { this.failedJobs = failedJobs; }
    }

    /**
     * 单个分组执行结果
     * 
     * 包含：
     * - 分组基本信息（名称、总作业数）
     * - 执行统计（成功、失败数量）
     * - 执行状态（错误信息、耗时、取消状态、停止状态）
     * - 失败作业列表
     */
    public static class GroupJobResult {
        
        // ==================== 基本信息 ====================
        private String groupName;
        private int totalJobs;
        
        // ==================== 执行统计 ====================
        private int successCount;
        private int failureCount;
        
        // ==================== 执行状态 ====================
        private String errorMessage;
        private long duration;
        private boolean cancelled;
        private boolean stopped;
        
        // ==================== 失败详情 ====================
        private List<String> failedJobs = new ArrayList<>();

        /**
         * 增加成功作业计数
         * 注意：totalJobs 只设置一次，不在此处累加
         */
        public void incrementSuccessCount() {
            successCount++;
        }

        /**
         * 增加失败作业计数
         * 注意：totalJobs 只设置一次，不在此处累加
         */
        public void incrementFailureCount() {
            failureCount++;
        }

        /**
         * 添加失败作业信息
         * 
         * @param jobCode 作业代码
         * @param errorMessage 错误信息
         */
        public void addFailedJob(String jobCode, String errorMessage) {
            failedJobs.add(jobCode + ":" + errorMessage);
        }

        // ==================== 状态查询方法 ====================
        public boolean isCancelled() { return cancelled; }
        public boolean isStopped() { return stopped; }
        
        // ==================== Getter 和 Setter 方法 ====================
        public String getGroupName() { return groupName; }
        public void setGroupName(String groupName) { this.groupName = groupName; }
        
        public int getTotalJobs() { return totalJobs; }
        public void setTotalJobs(int totalJobs) { this.totalJobs = totalJobs; }
        
        public int getSuccessCount() { return successCount; }
        public void setSuccessCount(int successCount) { this.successCount = successCount; }
        
        public int getFailureCount() { return failureCount; }
        public void setFailureCount(int failureCount) { this.failureCount = failureCount; }
        
        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
        
        public long getDuration() { return duration; }
        public void setDuration(long duration) { this.duration = duration; }
        
        public List<String> getFailedJobs() { return failedJobs; }
        public void setFailedJobs(List<String> failedJobs) { this.failedJobs = failedJobs; }
        
        public void setCancelled(boolean cancelled) { this.cancelled = cancelled; }
        public void setStopped(boolean stopped) { this.stopped = stopped; }
    }

    /**
     * 通知内容
     * 
     * 包含：
     * - 分组执行统计
     * - 作业执行统计
     * - 执行时间
     * - 分组结果详情
     */
    public static class NotificationContent {
        
        // ==================== 分组统计 ====================
        private int totalGroups;
        private int successGroups;
        private int failureGroups;
        
        // ==================== 作业统计 ====================
        private int totalJobs;
        private int successJobs;
        private int failureJobs;
        
        // ==================== 执行信息 ====================
        private Date executionTime;
        private Map<String, GroupJobResult> groupResults;

        // ==================== Getter 和 Setter 方法 ====================
        public int getTotalGroups() { return totalGroups; }
        public void setTotalGroups(int totalGroups) { this.totalGroups = totalGroups; }
        
        public int getSuccessGroups() { return successGroups; }
        public void setSuccessGroups(int successGroups) { this.successGroups = successGroups; }
        
        public int getFailureGroups() { return failureGroups; }
        public void setFailureGroups(int failureGroups) { this.failureGroups = failureGroups; }
        
        public int getTotalJobs() { return totalJobs; }
        public void setTotalJobs(int totalJobs) { this.totalJobs = totalJobs; }
        
        public int getSuccessJobs() { return successJobs; }
        public void setSuccessJobs(int successJobs) { this.successJobs = successJobs; }
        
        public int getFailureJobs() { return failureJobs; }
        public void setFailureJobs(int failureJobs) { this.failureJobs = failureJobs; }
        
        public Date getExecutionTime() { return executionTime; }
        public void setExecutionTime(Date executionTime) { this.executionTime = executionTime; }
        
        public Map<String, GroupJobResult> getGroupResults() { return groupResults; }
        public void setGroupResults(Map<String, GroupJobResult> groupResults) { this.groupResults = groupResults; }
    }
    
    /**
     * 获取执行器地址
     * 
     * 格式：IP地址:端口号
     * 默认端口：8081
     * 
     * @return 执行器地址字符串
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

    /**
     * 🎯 系统健康检查 - 定期检查作业组和作业状态一致性
     * 
     * 检查内容：
     * 1. 作业组状态一致性
     * 2. 作业状态一致性
     * 3. 自动修复不健康状态
     * 
     * 执行频率：由 XXL-JOB 调度器控制
     */
    @XxlJob("systemHealthCheck")
    public void systemHealthCheck() {
        try {
            logger.info("开始系统健康检查");
            long startTime = System.currentTimeMillis();
            
            // 检查作业组状态一致性
            GroupHealthCheckResult groupResult = checkGroupsHealth();
            
            // 检查作业状态一致性
            JobHealthCheckResult jobResult = checkJobsHealth();
            
            // 输出检查结果
            long durationMs = System.currentTimeMillis() - startTime;
            int durationSeconds = (int) (durationMs / 1000);
            logHealthCheckResult(groupResult, jobResult, durationSeconds);
            
        } catch (Exception e) {
            logger.error("系统健康检查失败", e);
        }
    }
    
    /**
     * 检查作业组健康状态
     * 
     * @return 作业组健康检查结果
     */
    private GroupHealthCheckResult checkGroupsHealth() {
        Set<String> activeGroups = jobControlService.getActiveJobGroups();
        int totalGroups = activeGroups.size();
        int healthyGroups = 0;
        int unhealthyGroups = 0;
        
        for (String groupKey : activeGroups) {
            try {
                String groupName = CacheKeyUtils.extractGroupName(groupKey);
                String batchNo = CacheKeyUtils.extractBatchNo(groupKey);
                if (isGroupStatusHealthy(groupName,batchNo)) {
                    healthyGroups++;
                    logger.debug("作业组 {} 状态健康", groupName);
                } else {
                    unhealthyGroups++;
                    logger.warn("作业组 {} 状态不健康，进行修复", groupName);
                    fixGroupStatus(groupName,"");
                }
            } catch (Exception e) {
                logger.error("检查作业组 {} 状态时发生异常", groupKey, e);
                unhealthyGroups++;
            }
        }
        
        return new GroupHealthCheckResult(totalGroups, healthyGroups, unhealthyGroups);
    }
    
    /**
     * 检查作业健康状态
     * 
     * @return 作业健康检查结果
     */
    private JobHealthCheckResult checkJobsHealth() {
        Set<String> activeGroups = jobControlService.getActiveJobGroups();
        int totalJobs = 0;
        int healthyJobs = 0;
        int unhealthyJobs = 0;
        
        for (String groupKey : activeGroups) {
            try {
                String groupName = CacheKeyUtils.extractGroupName(groupKey);
                String batchNo = CacheKeyUtils.extractBatchNo(groupKey);
                List<JobDef> groupJobs = jobDefService.getJobsByGroupOrdered(groupName);
                if (groupJobs != null) {
                    for (JobDef job : groupJobs) {
                        totalJobs++;
                        if (isJobStatusHealthy(job.getJobCode(),batchNo)) {
                            healthyJobs++;
                        } else {
                            unhealthyJobs++;
                            logger.warn("作业 {} 状态不健康，进行修复", job.getJobCode());
                            fixJobStatus(job.getJobCode(),batchNo);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("检查作业组 {} 内作业状态时发生异常", groupKey, e);
            }
        }
        
        return new JobHealthCheckResult(totalJobs, healthyJobs, unhealthyJobs);
    }
    
    /**
     * 记录健康检查结果
     * @param groupResult 作业组检查结果
     * @param jobResult 作业检查结果
     * @param duration 检查耗时（秒）
     */
    private void logHealthCheckResult(GroupHealthCheckResult groupResult, 
                                    JobHealthCheckResult jobResult, int duration) {
        logger.info("系统健康检查完成，耗时: {}秒", duration);
        logger.info("作业组检查结果: 总计={}, 健康={}, 不健康={}", 
                   groupResult.totalGroups, groupResult.healthyGroups, groupResult.unhealthyGroups);
        logger.info("作业检查结果: 总计={}, 健康={}, 不健康={}", 
                   jobResult.totalJobs, jobResult.healthyJobs, jobResult.unhealthyJobs);
    }
    
    /**
     * 作业组健康检查结果
     */
    private static class GroupHealthCheckResult {
        final int totalGroups, healthyGroups, unhealthyGroups;
        
        GroupHealthCheckResult(int totalGroups, int healthyGroups, int unhealthyGroups) {
            this.totalGroups = totalGroups;
            this.healthyGroups = healthyGroups;
            this.unhealthyGroups = unhealthyGroups;
        }
    }
    
    /**
     * 作业健康检查结果
     */
    private static class JobHealthCheckResult {
        final int totalJobs, healthyJobs, unhealthyJobs;
        
        JobHealthCheckResult(int totalJobs, int healthyJobs, int unhealthyJobs) {
            this.totalJobs = totalJobs;
            this.healthyJobs = healthyJobs;
            this.unhealthyJobs = unhealthyJobs;
        }
    }
    
    /**
     * 检查作业组状态是否健康
     * 
     * 健康状态定义：
     * - STOPPED: 所有标志都为 false
     * - PAUSED: 暂停标志为 true，其他标志为 false
     * - CANCELLED: 取消标志为 true，其他标志为 false
     * - RUNNING: 所有标志都为 false
     * 
     * @param groupName 作业组名称
     * @return 是否健康
     */
    private boolean isGroupStatusHealthy(String groupName,String batchNo) {
        try {
            String groupKey   = CacheKeyUtils.generateJobKey(groupName, batchNo);
            // 检查作业组是否存在
            if (!jobControlService.isJobGroupExists(groupKey)) {
                return false;
            }
            
            // 获取当前状态和标志
            ExecutionStatus status = jobControlService.getGroupStatus(groupName,batchNo);
            boolean isPaused = jobControlService.isGroupPaused(groupName,batchNo);
            boolean isStopped = jobControlService.isGroupStopped(groupName,batchNo);
            boolean isCancelled = jobControlService.isGroupCancelled(groupName,batchNo);
            
            // 状态一致性验证逻辑
            return validateGroupStatusConsistency(status, isPaused, isStopped, isCancelled);
            
        } catch (Exception e) {
            logger.error("检查作业组 {} 状态健康性时发生异常", groupName, e);
            return false;
        }
    }
    
    /**
     * 验证作业组状态一致性
     * 
     * @param status 当前状态
     * @param isPaused 是否暂停
     * @param isStopped 是否停止
     * @param isCancelled 是否取消
     * @return 是否一致
     */
    private boolean validateGroupStatusConsistency(ExecutionStatus status, boolean isPaused, 
                                                 boolean isStopped, boolean isCancelled) {
        switch (status) {
            case STOPPED:
                return !isPaused && !isStopped && !isCancelled;
            case PAUSED:
                return isPaused && !isStopped && !isCancelled;
            case CANCELLED:
                return !isPaused && !isStopped && isCancelled;
            case RUNNING:
                return !isPaused && !isStopped && !isCancelled;
            default:
                return true;
        }
    }
    
    /**
     * 检查作业状态是否健康
     * 
     * 健康状态定义：
     * - STOPPED: 所有标志都为 false
     * - PAUSED: 暂停标志为 true，其他标志为 false
     * - CANCELLED: 取消标志为 true，其他标志为 false
     * - RUNNING: 所有标志都为 false
     * 
     * @param jobCode 作业代码
     * @return 是否健康
     */
    private boolean isJobStatusHealthy(String jobCode,String batchNo) {
        try {
            String jobKey =  CacheKeyUtils.generateJobKey(jobCode, batchNo);
            // 检查作业是否存在
            if (!jobControlService.isJobExists(jobKey)) {
                return false;
            }
            
            // 获取当前状态和标志
            ExecutionStatus status = jobControlService.getJobStatus(jobCode,batchNo);
            boolean isPaused = jobControlService.isJobPaused(jobCode,batchNo);
            boolean isStopped = jobControlService.isJobStopped(jobCode,batchNo);
            boolean isCancelled = jobControlService.isJobCancelled(jobCode,batchNo);
            
            // 状态一致性验证逻辑
            return validateJobStatusConsistency(status, isPaused, isStopped, isCancelled);
            
        } catch (Exception e) {
            logger.error("检查作业 {} 状态健康性时发生异常", jobCode, e);
            return false;
        }
    }
    
    /**
     * 验证作业状态一致性
     * 
     * @param status 当前状态
     * @param isPaused 是否暂停
     * @param isStopped 是否停止
     * @param isCancelled 是否取消
     * @return 是否一致
     */
    private boolean validateJobStatusConsistency(ExecutionStatus status, boolean isPaused, 
                                               boolean isStopped, boolean isCancelled) {
        switch (status) {
            case STOPPED:
                return !isPaused && !isStopped && !isCancelled;
            case PAUSED:
                return isPaused && !isStopped && !isCancelled;
            case CANCELLED:
                return !isPaused && !isStopped && isCancelled;
            case RUNNING:
                return !isPaused && !isStopped && !isCancelled;
            default:
                return true;
        }
    }
    
    /**
     * 修复作业组状态
     * 
     * 修复策略：
     * 1. 重新加载作业组数据
     * 2. 清除所有标志
     * 3. 重置为初始状态
     * 
     * @param groupName 作业组名称
     */
    private void fixGroupStatus(String groupName,String batchNo) {
        try {
            logger.info("开始修复作业组 {} 状态", groupName);
            
            // 重新加载作业组数据
            jobControlService.loadJobGroupData(groupName,batchNo);
            
            // 重置为初始状态
            jobControlService.clearGroupFlags(groupName,batchNo);
            
            logger.info("作业组 {} 状态修复完成", groupName);
            
        } catch (Exception e) {
            logger.error("修复作业组 {} 状态失败", groupName, e);
        }
    }
    
    /**
     * 修复作业状态
     * 
     * 修复策略：
     * 1. 重新加载作业数据
     * 2. 清除所有标志
     * 3. 重置为初始状态
     * 
     * @param jobCode 作业代码
     */
    private void fixJobStatus(String jobCode,String batchNo) {
        try {
            logger.info("开始修复作业 {} 状态", jobCode);
            
            // 重新加载作业数据
            jobControlService.loadJobData(jobCode,batchNo);
            
            // 重置为初始状态
            jobControlService.clearJobFlags(jobCode,batchNo);
            
            logger.info("作业 {} 状态修复完成", jobCode);
            
        } catch (Exception e) {
            logger.error("修复作业 {} 状态失败", jobCode, e);
        }
    }
} 