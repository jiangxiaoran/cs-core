package com.aia.gdp.listener;

import com.aia.gdp.event.JobControlEvent;
import com.aia.gdp.model.JobExecutionInfo;
import com.aia.gdp.service.JobControlService;
import com.aia.gdp.common.CacheKeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.aia.gdp.model.JobDef;
import com.aia.gdp.service.JobDefService;
import java.util.stream.Collectors;

/**
 * 作业控制事件监听器
 * 负责管理作业执行信息，不直接管理执行状态
 * 采用事件驱动架构，解耦组件依赖
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
@Component
public class JobControlEventListener {
    
    private static final Logger logger = LoggerFactory.getLogger(JobControlEventListener.class);
    
    @Autowired
    private JobControlService jobControlService;
    
    @Autowired
    private JobDefService jobDefService;
    
    // 作业执行信息映射
    // 键值：jobCode + "_" + batchNo
    private final Map<String, JobExecutionInfo> jobExecutionInfoMap = new ConcurrentHashMap<>();
    
    // 作业组作业映射
    // 键值：groupName + "_" + batchNo
    private final Map<String, List<JobExecutionInfo>> groupJobMapping = new ConcurrentHashMap<>();
    
    /**
     * 注册作业执行信息
     */
    public void registerJobExecution(String jobCode, String batchNo, String groupName) {
        try {
            // 创建作业执行信息
            JobExecutionInfo info = new JobExecutionInfo(jobCode, batchNo, groupName);
            
            // 使用 jobCode + "_" + batchNo 作为键值
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            jobExecutionInfoMap.put(jobKey, info);
            
            // 使用 groupName + "_" + batchNo 作为键值
            String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
            groupJobMapping.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(info);
            
            logger.info("注册作业执行信息: {} (批次: {}, 组: {})", jobKey, batchNo, groupName);
            
        } catch (Exception e) {
            logger.error("注册作业执行信息失败: {}", jobCode, e);
        }
    }
    
    /**
     * 清理已完成的作业信息
     */
    public void cleanupCompletedJob(String jobCode, String batchNo) {
        try {
            String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
            JobExecutionInfo info = jobExecutionInfoMap.remove(jobKey);
            
            if (info != null) {
                // 从组映射中移除
                String groupName = info.getGroupName();
                String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
                List<JobExecutionInfo> groupJobs = groupJobMapping.get(groupKey);
                
                if (groupJobs != null) {
                    groupJobs.remove(info);
                    // 如果组中没有作业了，清理组映射
                    if (groupJobs.isEmpty()) {
                        groupJobMapping.remove(groupKey);
                    }
                }
                
                logger.info("清理作业执行信息: {} (组: {})", jobKey, groupKey);
            }
        } catch (Exception e) {
            logger.error("清理作业执行信息失败: {}", jobCode, e);
        }
    }
    
    /**
     * 获取作业执行信息
     */
    public JobExecutionInfo getJobExecutionInfo(String jobCode, String batchNo) {
        String jobKey = CacheKeyUtils.generateJobKey(jobCode, batchNo);
        return jobExecutionInfoMap.get(jobKey);
    }
    
    /**
     * 获取作业组的所有作业
     */
    public List<JobExecutionInfo> getGroupJobs(String groupName, String batchNo) {
        String groupKey = CacheKeyUtils.generateGroupKey(groupName, batchNo);
        return groupJobMapping.getOrDefault(groupKey, new ArrayList<>());
    }
    
    /**
     * 清理批次数据
     */
    public void cleanupBatchData(String batchNo) {
        try {
            logger.info("开始清理批次数据: {}", batchNo);
            
            // 清理作业执行信息
            jobExecutionInfoMap.entrySet().removeIf(entry -> {
                String key = entry.getKey();
                return CacheKeyUtils.isKeyBelongsToBatch(key, batchNo);
            });
            
            // 清理组作业映射
            groupJobMapping.entrySet().removeIf(entry -> {
                String key = entry.getKey();
                return CacheKeyUtils.isKeyBelongsToBatch(key, batchNo);
            });
            
            logger.info("批次数据清理完成: {}", batchNo);
            
        } catch (Exception e) {
            logger.error("清理批次数据失败: {}", batchNo, e);
        }
    }
    
    /**
     * 检查作业是否被请求停止
     * 现在通过 JobControlService 来检查状态
     */
    public boolean isJobStopRequested(String jobCode, String batchNo) {
        return jobControlService.isJobStopped(jobCode, batchNo);
    }
    
    /**
     * 检查作业是否被请求取消
     */
    public boolean isJobCancelRequested(String jobCode, String batchNo) {
        return jobControlService.isJobCancelled(jobCode, batchNo);
    }
    
    /**
     * 检查作业是否被请求暂停
     */
    public boolean isJobPauseRequested(String jobCode, String batchNo) {
        return jobControlService.isJobPaused(jobCode, batchNo);
    }
    
    /**
     * 获取作业组状态
     */
    public JobControlService.ExecutionStatus getGroupStatus(String groupName, String batchNo) {
        return jobControlService.getGroupStatus(groupName, batchNo);
    }
    
    /**
     * 获取作业状态
     */
    public JobControlService.ExecutionStatus getJobStatus(String jobCode, String batchNo) {
        return jobControlService.getJobStatus(jobCode, batchNo);
    }
    
    /**
     * 从数据库补充作业信息
     */
    public void supplementJobFromDatabase(String jobCode, String groupName) {
        try {
            JobDef dbJob = jobDefService.getJobByCode(jobCode);
            if (dbJob != null) {
                // 创建作业执行信息
                JobExecutionInfo info = new JobExecutionInfo(jobCode, "", groupName);
                
                // 使用空批次号作为键值（向后兼容）
                String jobKey = CacheKeyUtils.generateSafeJobKey(jobCode, "");
                jobExecutionInfoMap.put(jobKey, info);
                
                // 添加到组映射
                String groupKey = CacheKeyUtils.generateSafeGroupKey(groupName, "");
                groupJobMapping.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(info);
                
                logger.debug("从数据库补充作业: {} (组: {})", dbJob.getJobCode(), groupName);
            }
        } catch (Exception e) {
            logger.error("从数据库补充作业失败: {} (组: {})", jobCode, groupName, e);
        }
    }
    
    /**
     * 从数据库获取作业信息
     */
    public JobDef getJobFromDatabase(String jobCode, String groupName) {
        try {
            JobDef jobDef = jobDefService.getJobByCode(jobCode);
            if (jobDef != null) {
                logger.debug("从数据库获取作业: {} (组: {})", jobDef.getJobCode(), groupName);
            }
            return jobDef;
        } catch (Exception e) {
            logger.error("从数据库获取作业失败: {} (组: {})", jobCode, groupName, e);
            return null;
        }
    }
    
    /**
     * 获取所有作业执行信息
     */
    public List<JobExecutionInfo> getAllJobExecutionInfo() {
        return new ArrayList<>(jobExecutionInfoMap.values());
    }
    
    /**
     * 获取所有作业组映射
     */
    public Map<String, List<JobExecutionInfo>> getAllGroupJobMapping() {
        return new HashMap<>(groupJobMapping);
    }
    
    /**
     * 获取作业执行信息统计
     */
    public Map<String, Object> getJobExecutionStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalJobs", jobExecutionInfoMap.size());
        stats.put("totalGroups", groupJobMapping.size());
        
        // 按批次统计作业数量
        Map<String, Long> batchCounts = jobExecutionInfoMap.values().stream()
            .collect(Collectors.groupingBy(
                JobExecutionInfo::getBatchNo,
                Collectors.counting()
            ));
        stats.put("batchCounts", batchCounts);
        
        // 按组统计作业数量
        Map<String, Long> groupCounts = jobExecutionInfoMap.values().stream()
            .collect(Collectors.groupingBy(
                JobExecutionInfo::getGroupName,
                Collectors.counting()
            ));
        stats.put("groupCounts", groupCounts);
        
        return stats;
    }
    
    // ==================== 事件监听方法 ====================
    
    /**
     * 监听作业执行注册事件
     */
    @EventListener
    @Async
    public void handleJobExecutionRegistered(JobControlEvent.JobExecutionRegisteredEvent event) {
        try {
            logger.info("收到作业执行注册事件: {} (批次: {}, 组: {})", 
                event.getJobCode(), event.getBatchNo(), event.getGroupName());
            
            // 注册作业执行信息
            registerJobExecution(event.getJobCode(), event.getBatchNo(), event.getGroupName());
            
        } catch (Exception e) {
            logger.error("处理作业执行注册事件失败: {}", event, e);
        }
    }
    
    /**
     * 监听作业开始事件
     */
    @EventListener
    @Async
    public void handleJobStarted(JobControlEvent.JobStartedEvent event) {
        try {
            logger.info("收到作业开始事件: {} (批次: {})", 
                event.getJobCode(), event.getBatchNo());
            
            // 这里可以添加作业开始时的逻辑
            // 比如更新作业状态、记录开始时间等
            
        } catch (Exception e) {
            logger.error("处理作业开始事件失败: {}", event, e);
        }
    }
    
    /**
     * 监听作业执行完成事件
     */
    @EventListener
    @Async
    public void handleJobExecutionCompleted(JobControlEvent.JobExecutionCompletedEvent event) {
        try {
            logger.info("收到作业执行完成事件: {}", event.getJobCode());
            
            // 由于JobExecutionCompletedEvent没有批次号，我们需要从其他地方获取
            // 或者使用空批次号进行清理（向后兼容）
            cleanupCompletedJob(event.getJobCode(), event.getBatchNo());
            
        } catch (Exception e) {
            logger.error("处理作业执行完成事件失败: {}", event, e);
        }
    }
} 