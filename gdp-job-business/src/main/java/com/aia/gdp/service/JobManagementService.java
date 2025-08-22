package com.aia.gdp.service;

import com.aia.gdp.dto.JobListRequest;
import com.aia.gdp.model.JobDef;

import java.util.Map;

/**
 * 任务管理服务接口
 * 
 * @author andy
 * @date 2025-08-30
 */
public interface JobManagementService {
    
    /**
     * 获取任务列表（分页）
     */
    Map<String, Object> getJobList(JobListRequest request);
    
    /**
     * 获取任务详情
     */
    JobDef getJobDetail(Long jobId);
    
    /**
     * 创建任务
     */
    Long createJob(JobDef jobDef);
    
    /**
     * 更新任务
     */
    boolean updateJob(Long jobId, JobDef jobDef);
    
    /**
     * 删除任务
     */
    boolean deleteJob(Long jobId);
    
    /**
     * 检查任务是否存在
     */
    boolean jobExists(Long jobId);
    
    /**
     * 检查任务代码是否已存在
     */
    boolean jobCodeExists(String jobCode);
} 