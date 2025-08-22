package com.aia.gdp.service.impl;

import com.aia.gdp.dto.JobListRequest;
import com.aia.gdp.mapper.JobDefMapper;
import com.aia.gdp.model.JobDef;
import com.aia.gdp.service.JobManagementService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务管理服务实现类
 * 
 * @author andy
 * @date 2025-08-30
 */
@Service
public class JobManagementServiceImpl implements JobManagementService {
    
    private static final Logger logger = LoggerFactory.getLogger(JobManagementServiceImpl.class);
    
    @Autowired
    private JobDefMapper jobDefMapper;
    
    @Override
    public Map<String, Object> getJobList(JobListRequest request) {
        try {
            // 构建查询条件
            QueryWrapper<JobDef> queryWrapper = new QueryWrapper<>();
            
            if (StringUtils.hasText(request.getJobCode())) {
                queryWrapper.like("job_code", request.getJobCode());
            }
            
            if (StringUtils.hasText(request.getJobName())) {
                queryWrapper.like("job_name", request.getJobName());
            }
            
            if (StringUtils.hasText(request.getJobGroup())) {
                queryWrapper.eq("job_group", request.getJobGroup());
            }
            
            if (StringUtils.hasText(request.getStatus())) {
                queryWrapper.eq("status", request.getStatus());
            }
            
            if (StringUtils.hasText(request.getJobType())) {
                queryWrapper.eq("job_type", request.getJobType());
            }
            
            if (request.getIsActive() != null) {
                queryWrapper.eq("is_active", request.getIsActive());
            }
            
            // 按创建时间倒序排列
            queryWrapper.orderByDesc("create_time");
            
            // 分页查询
            Page<JobDef> page = new Page<>(request.getCurrent(), request.getPageSize());
            IPage<JobDef> result = jobDefMapper.selectPage(page, queryWrapper);
            
            // 构建返回结果
            Map<String, Object> data = new HashMap<>();
            data.put("list", result.getRecords());
            data.put("total", result.getTotal());
            data.put("current", result.getCurrent());
            data.put("pageSize", result.getSize());
            data.put("pages", result.getPages());
            
            return data;
            
        } catch (Exception e) {
            logger.error("获取任务列表失败", e);
            throw new RuntimeException("获取任务列表失败: " + e.getMessage());
        }
    }
    
    @Override
    public JobDef getJobDetail(Long jobId) {
        try {
            JobDef jobDef = jobDefMapper.selectById(jobId);
            if (jobDef == null) {
                throw new RuntimeException("任务不存在");
            }
            return jobDef;
        } catch (Exception e) {
            logger.error("获取任务详情失败, jobId: {}", jobId, e);
            throw new RuntimeException("获取任务详情失败: " + e.getMessage());
        }
    }
    
    @Override
    @Transactional
    public Long createJob(JobDef jobDef) {
        try {
            // 检查任务代码是否已存在
            if (jobCodeExists(jobDef.getJobCode())) {
                throw new RuntimeException("任务代码已存在: " + jobDef.getJobCode());
            }
            
            // 设置默认值
            if (jobDef.getCreateTime() == null) {
                jobDef.setCreateTime(new Date());
            }
            if (jobDef.getUpdateTime() == null) {
                jobDef.setUpdateTime(new Date());
            }
            if (jobDef.getIsActive() == null) {
                jobDef.setIsActive(true);
            }
            if (jobDef.getStatus() == null) {
                jobDef.setStatus("ACTIVE");
            }
            
            // 确保状态值正确
            if ("active".equalsIgnoreCase(jobDef.getStatus())) {
                jobDef.setStatus("ACTIVE");
            } else if ("inactive".equalsIgnoreCase(jobDef.getStatus())) {
                jobDef.setStatus("INACTIVE");
            }
            
            // 处理时间字段
            if (jobDef.getScheduleTimeStr() != null && !jobDef.getScheduleTimeStr().trim().isEmpty()) {
                try {
                    java.sql.Time sqlTime = java.sql.Time.valueOf(jobDef.getScheduleTimeStr());
                    jobDef.setScheduleTime(sqlTime);
                } catch (Exception e) {
                    logger.warn("时间格式错误: {}", jobDef.getScheduleTimeStr());
                    jobDef.setScheduleTime(null);
                }
            }
            
            // 插入数据
            int result = jobDefMapper.insert(jobDef);
            if (result <= 0) {
                throw new RuntimeException("创建任务失败");
            }
            
            logger.info("创建任务成功, jobId: {}, jobCode: {}", jobDef.getJobId(), jobDef.getJobCode());
            return jobDef.getJobId();
            
        } catch (Exception e) {
            logger.error("创建任务失败", e);
            throw new RuntimeException("创建任务失败: " + e.getMessage());
        }
    }
    
    @Override
    @Transactional
    public boolean updateJob(Long jobId, JobDef jobDef) {
        try {
            // 检查任务是否存在
            if (!jobExists(jobId)) {
                throw new RuntimeException("任务不存在");
            }
            
            // 设置更新时间
            jobDef.setUpdateTime(new Date());
            jobDef.setJobId(jobId);
            
            // 更新数据
            int result = jobDefMapper.updateById(jobDef);
            if (result <= 0) {
                throw new RuntimeException("更新任务失败");
            }
            
            logger.info("更新任务成功, jobId: {}", jobId);
            return true;
            
        } catch (Exception e) {
            logger.error("更新任务失败, jobId: {}", jobId, e);
            throw new RuntimeException("更新任务失败: " + e.getMessage());
        }
    }
    
    @Override
    @Transactional
    public boolean deleteJob(Long jobId) {
        try {
            // 检查任务是否存在
            if (!jobExists(jobId)) {
                throw new RuntimeException("任务不存在");
            }
            
            // 删除数据
            int result = jobDefMapper.deleteById(jobId);
            if (result <= 0) {
                throw new RuntimeException("删除任务失败");
            }
            
            logger.info("删除任务成功, jobId: {}", jobId);
            return true;
            
        } catch (Exception e) {
            logger.error("删除任务失败, jobId: {}", jobId, e);
            throw new RuntimeException("删除任务失败: " + e.getMessage());
        }
    }
    
    @Override
    public boolean jobExists(Long jobId) {
        return jobDefMapper.selectById(jobId) != null;
    }
    
    @Override
    public boolean jobCodeExists(String jobCode) {
        if (!StringUtils.hasText(jobCode)) {
            return false;
        }
        
        QueryWrapper<JobDef> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("job_code", jobCode);
        return jobDefMapper.selectCount(queryWrapper) > 0;
    }
} 